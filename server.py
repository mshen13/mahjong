#!/usr/bin/env python3
"""Multiplayer mahjong server: 2 humans vs 2 bots, room-based, over WebSocket."""

import asyncio
import json
import os
import random
import secrets
import string
from pathlib import Path

from aiohttp import web, WSMsgType

SUITS = ["C", "B", "O"]
WINDS = ["EW", "SW", "WW", "NW"]
HONOR_ORDER = WINDS
AVATARS = ["rabbit", "monkey", "tiger", "cub"]
SEAT_NAMES = ["Player 1", "Player 2", "Bot A", "Bot B"]
DISCARD_ANIM_SECONDS = 1.9
CALL_BANNER_SECONDS = 3.3
BOT_DRAW_PAUSE = 0.6


def clean_name(raw, fallback, taken=()):
    name = " ".join(str(raw or "").split())[:16] or fallback
    if name.lower() in (t.lower() for t in taken):
        name = f"{name} (2)"
    return name


def is_suited(tile):
    return len(tile) == 2 and tile[0].isdigit() and tile[1] in SUITS


def tile_sort_value(tile):
    if is_suited(tile):
        return SUITS.index(tile[1]) * 100 + int(tile[0])
    return 1000 + HONOR_ORDER.index(tile)


def sort_tiles(tiles):
    return sorted(tiles, key=tile_sort_value)


def build_wall():
    tiles = []
    for suit in SUITS:
        for rank in range(1, 10):
            tiles.extend([f"{rank}{suit}"] * 4)
    for honor in HONOR_ORDER:
        tiles.extend([honor] * 4)
    random.shuffle(tiles)
    return tiles


def counts(tiles):
    c = {}
    for t in tiles:
        c[t] = c.get(t, 0) + 1
    return c


def can_decompose(cnt, needed):
    if needed == 0:
        return True
    present = sorted((t for t, v in cnt.items() if v > 0), key=tile_sort_value)
    if not present:
        return False
    tile = present[0]

    if cnt[tile] >= 3:
        cnt[tile] -= 3
        if can_decompose(cnt, needed - 1):
            cnt[tile] += 3
            return True
        cnt[tile] += 3

    if is_suited(tile):
        rank, suit = int(tile[0]), tile[1]
        if rank <= 7:
            t2, t3 = f"{rank + 1}{suit}", f"{rank + 2}{suit}"
            if cnt.get(t2, 0) > 0 and cnt.get(t3, 0) > 0:
                cnt[tile] -= 1
                cnt[t2] -= 1
                cnt[t3] -= 1
                if can_decompose(cnt, needed - 1):
                    cnt[tile] += 1
                    cnt[t2] += 1
                    cnt[t3] += 1
                    return True
                cnt[tile] += 1
                cnt[t2] += 1
                cnt[t3] += 1
    return False


def is_winning_hand(tiles, melds):
    sets_needed = 4 - melds
    if len(tiles) != sets_needed * 3 + 2:
        return False
    cnt = counts(tiles)
    for pair_tile in list(cnt.keys()):
        if cnt[pair_tile] >= 2:
            cnt[pair_tile] -= 2
            if can_decompose(cnt, sets_needed):
                cnt[pair_tile] += 2
                return True
            cnt[pair_tile] += 2
    return False


def find_chow_options(hand, tile):
    if not is_suited(tile):
        return []
    rank, suit = int(tile[0]), tile[1]
    cnt = counts(hand)
    options = []
    for d1, d2 in ((-2, -1), (-1, 1), (1, 2)):
        r1, r2 = rank + d1, rank + d2
        if 1 <= r1 <= 9 and 1 <= r2 <= 9:
            t1, t2 = f"{r1}{suit}", f"{r2}{suit}"
            if cnt.get(t1, 0) > 0 and cnt.get(t2, 0) > 0:
                options.append(sorted([t1, t2], key=tile_sort_value))
    return options


def tile_score(tile, hand):
    cnt = counts(hand)
    score = (cnt[tile] - 1) * 10
    if is_suited(tile):
        rank, suit = int(tile[0]), tile[1]
        for d in (-2, -1, 1, 2):
            r2 = rank + d
            if 1 <= r2 <= 9:
                score += cnt.get(f"{r2}{suit}", 0) * (3 if abs(d) == 1 else 1)
    return score


def ai_discard_choice(hand):
    unique = list(set(hand))
    unique.sort(key=lambda t: tile_score(t, hand))
    return unique[0]


class Player:
    def __init__(self, seat, is_human):
        self.seat = seat
        self.is_human = is_human
        self.name = SEAT_NAMES[seat]
        self.avatar = AVATARS[seat]
        self.hand = []
        self.melds = []
        self.discards = []
        self.ws = None
        self.token = None  # proves seat ownership when reclaiming it


class Room:
    def __init__(self, code):
        self.code = code
        self.players = [Player(i, i < 2) for i in range(4)]
        self.wall = []
        self.pending = None  # {"kind": "discard"|"call", "seat": int}
        self.pending_future = None
        self.pending_payload = None  # last prompt sent, replayed on rejoin
        # Call prompts are open to every eligible human at once: seat -> {"future", "payload"}
        self.pending_calls = {}
        self.started = False
        self.finished = False
        self.lock = asyncio.Lock()

    def human_sockets(self):
        return [p.ws for p in self.players if p.is_human and p.ws is not None]


ROOMS = {}


def make_room_code():
    while True:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
        if code not in ROOMS:
            return code


async def send_json(ws, payload):
    if ws is not None and not ws.closed:
        await ws.send_json(payload)


async def broadcast(room, payload):
    for ws in room.human_sockets():
        await send_json(ws, payload)


async def broadcast_log(room, parts):
    await broadcast(room, {"type": "log", "parts": parts})


async def send_log(ws, parts):
    await send_json(ws, {"type": "log", "parts": parts})


def public_player_view(p):
    return {
        "seat": p.seat,
        "name": p.name,
        "isHuman": p.is_human,
        "avatar": p.avatar,
        "handCount": len(p.hand),
        "melds": p.melds,
        "discards": p.discards,
    }


async def broadcast_state(room):
    base_players = [public_player_view(p) for p in room.players]
    for p in room.players:
        if p.is_human and p.ws is not None:
            await send_json(p.ws, {
                "type": "state",
                "mySeat": p.seat,
                "wall": len(room.wall),
                "players": base_players,
                "yourHand": sort_tiles(p.hand),
            })


async def broadcast_discard_anim(room, seat_idx, tile):
    await broadcast(room, {"type": "discard_anim", "seat": seat_idx, "tile": tile})


async def broadcast_call_banner(room, seat_idx, kind, tiles):
    await broadcast(room, {"type": "call_banner", "seat": seat_idx, "kind": kind, "tiles": tiles})


def own_kong_options(player):
    """Tiles this player can kong on their own turn: four in hand, or a fourth for an exposed pong."""
    options = [t for t in dict.fromkeys(player.hand) if player.hand.count(t) == 4]
    for meld in player.melds:
        if meld["kind"] == "pong" and meld["tiles"][0] in player.hand:
            options.append(meld["tiles"][0])
    return options


def apply_own_kong(player, tile):
    if player.hand.count(tile) == 4:
        for _ in range(4):
            player.hand.remove(tile)
        player.melds.append({"kind": "kong", "tiles": [tile] * 4})
        return
    for meld in player.melds:
        if meld["kind"] == "pong" and meld["tiles"][0] == tile:
            player.hand.remove(tile)
            meld["kind"] = "kong"
            meld["tiles"] = [tile] * 4
            return


async def request_discard(room, player):
    fut = asyncio.get_event_loop().create_future()
    room.pending = {"kind": "discard", "seat": player.seat}
    room.pending_future = fut
    room.pending_payload = {
        "type": "await_discard",
        "canWin": is_winning_hand(player.hand, len(player.melds)),
        "kongOptions": own_kong_options(player),
    }
    await send_json(player.ws, room.pending_payload)
    tile = await fut
    room.pending = None
    room.pending_future = None
    room.pending_payload = None
    return tile


async def request_call_action(room, discarder_idx, tile, player, is_next_player):
    error = ""
    try:
        while True:
            fut = asyncio.get_event_loop().create_future()
            payload = {
                "type": "await_call",
                "discarderIdx": discarder_idx,
                "tile": tile,
                "isNextPlayer": is_next_player,
                "canWin": is_winning_hand(player.hand + [tile], len(player.melds)),
                "canKong": player.hand.count(tile) == 3,
                "error": error,
            }
            room.pending_calls[player.seat] = {"future": fut, "payload": payload}
            await send_json(player.ws, payload)
            msg = await fut
            error = ""
            action = msg.get("action")

            if action == "skip":
                return {"type": "pass"}

            if action == "win":
                if is_winning_hand(player.hand + [tile], len(player.melds)):
                    return {"type": "win"}
                error = "That tile doesn't complete your hand."
                continue

            if action == "kong":
                if player.hand.count(tile) == 3:
                    return {"type": "kong"}
                error = "You can't kong this — you need three of it in your hand."
                continue

            if action == "pong":
                if player.hand.count(tile) >= 2:
                    return {"type": "pong"}
                error = "You can't pong this — you don't have a pair of it."
                continue

            if action == "chow":
                # Out of turn, a chow is only allowed when it completes the hand (a win).
                completes_hand = is_winning_hand(player.hand + [tile], len(player.melds))
                if not is_next_player and not completes_hand:
                    error = "You can't chow this — only the player immediately after the discarder can chow."
                    continue
                options = find_chow_options(player.hand, tile)
                pair = msg.get("pair")
                valid = pair is not None and any(
                    sorted(pair) == sorted(opt) for opt in options
                )
                if not valid:
                    error = "You can't chow this — you don't have two tiles to complete a run with it."
                    continue
                return {"type": "chow", "pair": pair, "win": not is_next_player}
    finally:
        room.pending_calls.pop(player.seat, None)


async def ask_humans_for_call(room, discarder_idx, tile, next_idx):
    """Prompt every human at once; the first valid pong/chow wins."""
    humans = [room.players[i] for i in range(4) if i != discarder_idx and room.players[i].is_human]
    tasks = {
        asyncio.ensure_future(
            request_call_action(room, discarder_idx, tile, p, p.seat == next_idx)
        ): p
        for p in humans
    }
    try:
        while tasks:
            done, _ = await asyncio.wait(tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                player = tasks.pop(t)
                result = t.result()
                if result["type"] == "pass":
                    continue
                for other_task, other in tasks.items():
                    other_task.cancel()
                    await send_json(other.ws, {"type": "call_closed"})
                tasks.clear()
                return {"idx": player.seat, "result": result}
        return None
    finally:
        for t in tasks:
            t.cancel()


async def check_calls(room, discarder_idx, tile):
    order = [(discarder_idx + i) % 4 for i in (1, 2, 3)]
    next_idx = order[0]

    human_call = await ask_humans_for_call(room, discarder_idx, tile, next_idx)

    if human_call and (
        human_call["result"]["type"] == "win" or human_call["result"].get("win")
    ):
        return {"kind": "win", "idx": human_call["idx"]}

    # Bots win automatically; humans only win by declaring it above.
    for idx in order:
        p = room.players[idx]
        if not p.is_human and is_winning_hand(p.hand + [tile], len(p.melds)):
            return {"kind": "win", "idx": idx}

    # A pong outranks a chow, so a bot's pong beats a human's chow.
    if human_call and human_call["result"]["type"] in ("pong", "kong"):
        return {"kind": human_call["result"]["type"], "idx": human_call["idx"]}

    for idx in order:
        p = room.players[idx]
        if not p.is_human and p.hand.count(tile) >= 2:
            return {"kind": "pong", "idx": idx}

    if human_call:
        return {"kind": "chow", "idx": human_call["idx"], "extra": human_call["result"]["pair"]}

    next_player = room.players[next_idx]
    if not next_player.is_human:
        options = find_chow_options(next_player.hand, tile)
        if options:
            return {"kind": "chow", "idx": next_idx, "extra": options[0]}

    return None


def apply_meld(player, discard, kind, extra):
    if kind == "pong":
        player.hand.remove(discard)
        player.hand.remove(discard)
        player.melds.append({"kind": "pong", "tiles": [discard, discard, discard]})
    elif kind == "kong":
        for _ in range(3):
            player.hand.remove(discard)
        player.melds.append({"kind": "kong", "tiles": [discard] * 4})
    elif kind == "chow":
        t1, t2 = extra
        player.hand.remove(t1)
        player.hand.remove(t2)
        player.melds.append({"kind": "chow", "tiles": sort_tiles([discard, t1, t2])})


async def end_game(room, winner_idx, method, discarder_idx=None):
    room.finished = True
    winner = room.players[winner_idx] if winner_idx is not None else None
    await broadcast(room, {
        "type": "game_over",
        "winnerIdx": winner_idx,
        "winnerName": winner.name if winner else None,
        "method": method,
        "discarderName": room.players[discarder_idx].name if discarder_idx is not None else None,
        "winningHand": sort_tiles(winner.hand) if winner else [],
        "melds": winner.melds if winner else [],
    })


async def run_game(room):
    room.wall = build_wall()
    for p in room.players:
        p.hand = [room.wall.pop() for _ in range(13)]

    current_idx = 0
    needs_draw = True
    await broadcast_state(room)

    while True:
        player = room.players[current_idx]

        if needs_draw:
            if not room.wall:
                await broadcast_log(room, ["Wall is empty! Game ends in a draw."])
                await end_game(room, None, "draw")
                return

            tile = room.wall.pop()
            player.hand.append(tile)

            if player.is_human:
                await send_log(player.ws, ["You drew: ", {"tile": tile}])
                for other in room.players:
                    if other.is_human and other.seat != player.seat:
                        await send_log(other.ws, [f"{player.name} draws a tile."])
            else:
                await broadcast_log(room, [f"{player.name} draws a tile."])
                await asyncio.sleep(BOT_DRAW_PAUSE)

            await broadcast_state(room)

            if not player.is_human and is_winning_hand(player.hand, len(player.melds)):
                await end_game(room, current_idx, "self-draw")
                return

        if player.is_human:
            discard = await request_discard(room, player)
            if discard is None:  # declared a self-draw win
                await end_game(room, current_idx, "self-draw")
                return
            if isinstance(discard, dict):  # declared a kong; draw a replacement, then discard
                apply_own_kong(player, discard["kong"])
                await broadcast_log(room, [player.name, " calls KONG on ", {"tile": discard["kong"]}, "!"])
                await broadcast_state(room)
                await broadcast_call_banner(room, current_idx, "kong", player.melds[-1]["tiles"])
                await asyncio.sleep(CALL_BANNER_SECONDS)
                needs_draw = True
                continue
        else:
            discard = ai_discard_choice(player.hand)

        player.hand.remove(discard)
        player.discards.append(discard)
        await broadcast_log(room, [player.name, " discards ", {"tile": discard}, "."])
        await broadcast_discard_anim(room, current_idx, discard)
        await asyncio.sleep(DISCARD_ANIM_SECONDS)
        await broadcast_state(room)

        call = await check_calls(room, current_idx, discard)
        if call and call["kind"] == "win":
            room.players[call["idx"]].hand.append(discard)
            await end_game(room, call["idx"], "ron", current_idx)
            return
        elif call:
            apply_meld(room.players[call["idx"]], discard, call["kind"], call.get("extra"))
            await broadcast_log(room, [
                room.players[call["idx"]].name,
                f" calls {call['kind'].upper()} on ",
                {"tile": discard},
                "!",
            ])
            await broadcast_state(room)
            new_meld = room.players[call["idx"]].melds[-1]
            await broadcast_call_banner(room, call["idx"], call["kind"], new_meld["tiles"])
            await asyncio.sleep(CALL_BANNER_SECONDS)
            current_idx = call["idx"]
            needs_draw = call["kind"] == "kong"  # a kong is followed by a replacement draw
        else:
            current_idx = (current_idx + 1) % 4
            needs_draw = True


async def handle_message(ws, ctx, data):
    msg_type = data.get("type")

    if msg_type == "create_room":
        code = make_room_code()
        room = Room(code)
        ROOMS[code] = room
        room.players[0].ws = ws
        room.players[0].token = secrets.token_urlsafe(16)
        room.players[0].name = clean_name(data.get("name"), SEAT_NAMES[0])
        if data.get("avatar") in AVATARS:
            room.players[0].avatar = data["avatar"]
        ctx["room"] = room
        ctx["seat"] = 0
        await send_json(ws, {
            "type": "joined",
            "room": code,
            "seat": 0,
            "waiting": True,
            "token": room.players[0].token,
        })
        return

    if msg_type == "join_room":
        code = (data.get("room") or "").strip().upper()
        room = ROOMS.get(code)
        if room is None:
            await send_json(ws, {"type": "error", "message": "That room code doesn't exist."})
            return
        if room.started:
            await send_json(ws, {"type": "error", "message": "That game is already in progress."})
            return
        if room.players[1].ws is not None:
            await send_json(ws, {"type": "error", "message": "That room is already full."})
            return
        room.players[1].ws = ws
        room.players[1].token = secrets.token_urlsafe(16)
        room.players[1].name = clean_name(
            data.get("name"), SEAT_NAMES[1], taken=[room.players[0].name, "Bot A", "Bot B"]
        )
        # Guest gets their pick unless the host already has it; bots take whatever is left.
        wanted = data.get("avatar")
        free = [a for a in AVATARS if a != room.players[0].avatar]
        room.players[1].avatar = wanted if wanted in free else free[0]
        leftovers = [a for a in AVATARS if a not in (room.players[0].avatar, room.players[1].avatar)]
        room.players[2].avatar, room.players[3].avatar = leftovers
        ctx["room"] = room
        ctx["seat"] = 1
        await send_json(ws, {
            "type": "joined",
            "room": code,
            "seat": 1,
            "waiting": False,
            "token": room.players[1].token,
        })
        await send_json(room.players[0].ws, {"type": "opponent_joined"})
        room.started = True
        asyncio.create_task(run_game(room))
        return

    if msg_type == "rejoin":
        code = (data.get("room") or "").strip().upper()
        seat = data.get("seat")
        room = ROOMS.get(code)
        if room is None or seat not in (0, 1) or room.finished:
            await send_json(ws, {"type": "rejoin_failed"})
            return
        player = room.players[seat]
        token = data.get("token")
        if not player.token or token != player.token:
            await send_json(ws, {"type": "rejoin_failed"})
            return
        stale = player.ws
        player.ws = ws
        if stale is not None and not stale.closed:
            asyncio.create_task(stale.close())
        ctx["room"] = room
        ctx["seat"] = seat
        await send_json(ws, {
            "type": "joined",
            "room": code,
            "seat": seat,
            "waiting": not room.started,
            "token": player.token,
        })
        if room.started:
            await broadcast_log(room, [f"{player.name} reconnected."])
            await broadcast_state(room)
            # Re-send the prompt they were on, or their turn would hang forever.
            if room.pending and room.pending["seat"] == seat and room.pending_payload:
                await send_json(ws, room.pending_payload)
            call = room.pending_calls.get(seat)
            if call:
                await send_json(ws, call["payload"])
        return

    room = ctx.get("room")
    seat = ctx.get("seat")
    if room is None or seat is None:
        return

    if msg_type == "declare_win":
        if room.pending == {"kind": "discard", "seat": seat}:
            player = room.players[seat]
            if is_winning_hand(player.hand, len(player.melds)) and not room.pending_future.done():
                room.pending_future.set_result(None)
        return

    if msg_type == "declare_kong":
        if room.pending == {"kind": "discard", "seat": seat}:
            player = room.players[seat]
            tile = data.get("tile")
            if tile in own_kong_options(player) and not room.pending_future.done():
                room.pending_future.set_result({"kong": tile})
        return

    if msg_type == "discard":
        if room.pending and room.pending == {"kind": "discard", "seat": seat}:
            player = room.players[seat]
            tile = data.get("tile")
            if tile in player.hand and not room.pending_future.done():
                room.pending_future.set_result(tile)
        return

    if msg_type == "call_action":
        call = room.pending_calls.get(seat)
        if call and not call["future"].done():
            call["future"].set_result(data)
        return


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    ctx = {"room": None, "seat": None}

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except ValueError:
                    continue
                await handle_message(ws, ctx, data)
            elif msg.type == WSMsgType.ERROR:
                break
    finally:
        room = ctx.get("room")
        seat = ctx.get("seat")
        if room is not None and seat is not None and room.players[seat].ws is ws:
            room.players[seat].ws = None
            if room.started and not room.finished:
                await broadcast_log(
                    room,
                    [f"{room.players[seat].name} dropped out — waiting for them to come back…"],
                )

    return ws


async def index_handler(request):
    return web.FileResponse(Path(__file__).parent / "public" / "index.html")


def main():
    app = web.Application()
    app.router.add_get("/", index_handler)
    app.router.add_get("/ws", ws_handler)
    port = int(os.environ.get("PORT", 8765))
    print(f"Mahjong multiplayer server running at http://localhost:{port}")
    web.run_app(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
