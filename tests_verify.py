"""Automated verification suite for TwitchMultiMiner fixes and multi-account orchestration."""
from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure TwitchMultiMiner is on sys.path
PROJECT_DIR = Path(__file__).resolve().parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from channel import Channel, Stream
from constants import DEFAULT_PRIORITY_GAMES, URLType
from inventory import DropsCampaign, TimedDrop
from orchestrator import MultiAccountOrchestrator
from radar import SharedCampaignRadar, scan_installed_pc_games
from settings import AccountProfileConfig, GlobalSettings
from utils import Game
from worker import AccountWorker


class _DummyResponse:
    def __init__(self, status: int, text_body: str = "") -> None:
        self.status = status
        self._text = text_body

    async def text(self, *_args, **_kwargs) -> str:
        return self._text

    async def read(self) -> bytes:
        return self._text.encode("utf8")

    async def json(self):
        import json

        return json.loads(self._text)

    def release(self) -> None:
        pass

    async def __aenter__(self) -> _DummyResponse:
        return self

    async def __aexit__(self, *_args) -> None:
        return None


def _make_campaign_payload(
    *,
    cid: str = "camp_1",
    name: str = "GTA 6 Reveal Drops",
    game_id: str = "999999",
    game_name: str = "Grand Theft Auto VI",
    acl_channels: list[dict] | None = None,
    required_minutes: int = 60,
) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "id": cid,
        "name": name,
        "status": "ACTIVE",
        "startAt": (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endAt": (now + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "accountLinkURL": "https://example.com",
        "self": {"isAccountConnected": True},
        "game": {"id": game_id, "name": game_name, "displayName": game_name, "boxArtURL": ""},
        "allow": {
            "isEnabled": bool(acl_channels),
            "channels": acl_channels,
        },
        "timeBasedDrops": [
            {
                "id": "drop_1",
                "name": "GTA 6 Exclusive Pack",
                "startAt": (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endAt": (now + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "requiredMinutesWatched": required_minutes,
                "preconditionDrops": [],
                "benefitEdges": [
                    {
                        "benefit": {
                            "id": "ben_1",
                            "name": "Vice City Jacket",
                            "distributionType": "DIRECT_ENTITLEMENT",
                            "imageAssetURL": "",
                        }
                    }
                ],
                "self": {
                    "dropInstanceID": None,
                    "isClaimed": False,
                    "currentMinutesWatched": 0,
                },
            }
        ],
    }


class TestTwitchMultiMiner(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmpdir.name)
        self.profiles_dir = self.tmp_path / "profiles"
        self.gs = GlobalSettings()
        self.radar = SharedCampaignRadar(self.gs)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def _make_worker(self, name: str, uid: int = 12345) -> AccountWorker:
        cfg = AccountProfileConfig.load(name, self.profiles_dir)
        cfg.user_id = uid
        cfg.username = f"user_{name}"
        cfg.save(self.profiles_dir)
        worker = AccountWorker(cfg, self.gs, self.radar, profiles_dir=self.profiles_dir)
        worker._auth_state.user_id = uid
        worker._auth_state.access_token = f"tok_{name}"
        return worker

    def test_stream_watch_payload_client_time_not_cached(self) -> None:
        async def run() -> None:
            worker = self._make_worker("acc1")
            ch = Channel(worker, id=777, login="kaicenat", display_name="KaiCenat", pinned=True)
            stream = Stream(
                ch,
                id=888,
                game={"id": "509658", "name": "Just Chatting"},
                viewers=250000,
                title="GTA 6 HYPE",
            )
            p1 = stream._watch_payload
            await asyncio.sleep(0.02)
            p2 = stream._watch_payload
            t1 = p1[0]["properties"]["client_time"]
            t2 = p2[0]["properties"]["client_time"]
            self.assertNotEqual(t1, t2, "client_time must update on every watch payload read")
            self.assertNotEqual(stream.spade_payload["data"], "")

        asyncio.run(run())

    def test_hls_head_only_last_chunk_and_reset_on_404(self) -> None:
        async def run() -> None:
            worker = self._make_worker("acc1")
            ch = Channel(worker, id=777, login="kaicenat", display_name="KaiCenat", pinned=True)
            stream = Stream(ch, id=888, game={"id": "509658", "name": "Just Chatting"}, viewers=100, title="test")
            stream._stream_url = URLType("https://video-weaver.example.com/playlist.m3u8")
            ch._stream = stream

            calls: list[tuple[str, str]] = []
            playlist_body = (
                "#EXTM3U\n"
                "#EXTINF:2.0,\nhttps://cdn.example.com/chunk1.ts\n"
                "#EXTINF:2.0,\nhttps://cdn.example.com/chunk2.ts\n"
                "#EXTINF:2.0,\nhttps://cdn.example.com/chunk3.ts\n"
            )

            def fake_request(method: str, url, **_kwargs):
                calls.append((method, str(url)))
                if method == "GET":
                    return _DummyResponse(200, playlist_body)
                return _DummyResponse(200, "")

            worker.request = fake_request  # type: ignore[method-assign]
            ok = await ch.send_watch()
            self.assertTrue(ok)
            head_calls = [c for c in calls if c[0] == "HEAD"]
            self.assertEqual(len(head_calls), 1, "Only the last HLS chunk should receive HEAD")
            self.assertEqual(head_calls[0][1], "https://cdn.example.com/chunk3.ts")

            # Now simulate expired playlist returning 403 -> _stream_url must reset to None
            worker.request = lambda method, url, **_kw: _DummyResponse(403, "")  # type: ignore[method-assign]
            ok2 = await ch.send_watch()
            self.assertFalse(ok2)
            self.assertIsNone(stream._stream_url)

        asyncio.run(run())

    def test_no_15_min_freeze_and_auto_generate_claim_id(self) -> None:
        async def run() -> None:
            worker = self._make_worker("acc_claim", uid=987654)
            payload = _make_campaign_payload(required_minutes=60)
            camp = DropsCampaign(worker, payload, {})
            drop: TimedDrop = camp.timed_drops["drop_1"]
            ch = Channel(worker, id=555, login="kaicenat", pinned=True)
            ch._stream = Stream(ch, id=1, game={"id": "999999", "name": "Grand Theft Auto VI"}, viewers=1000, title="GTA")

            # Simulate 25 delayed WS minutes (> old MAX_EXTRA_MINUTES=15 limit)
            for _ in range(25):
                drop._bump_minutes(ch)
            self.assertEqual(drop.extra_current_minutes, 25)
            self.assertTrue(drop.can_earn(ch), "Drop must stay earnable past 15 extra minutes")

            # Simulate reaching required minutes with claim_id missing -> ensure_claim generates UserID#CampaignID#DropID
            drop.update_minutes(60)
            self.assertTrue(drop.ready_to_claim)
            self.assertIsNone(drop.claim_id)
            worker.gql_request = AsyncMock(
                return_value={"data": {"claimDropRewards": {"status": "ELIGIBLE_FOR_ALL"}}}
            )  # type: ignore[method-assign]
            claimed = await drop.ensure_claim()
            self.assertTrue(claimed)
            self.assertEqual(drop.claim_id, "987654#camp_1#drop_1")
            self.assertTrue(drop.is_claimed)

        asyncio.run(run())

    def test_acl_and_verified_channel_can_earn_in_just_chatting(self) -> None:
        worker = self._make_worker("acc_acl")
        kai_acl = [{"id": "777001", "name": "kaicenat", "displayName": "KaiCenat"}]
        payload = _make_campaign_payload(
            cid="camp_gta_acl",
            name="GTA 6 Launch Drop",
            game_id="999999",
            game_name="Grand Theft Auto VI",
            acl_channels=kai_acl,
        )
        camp = DropsCampaign(worker, payload, {})
        ch_kai = Channel(worker, id=777001, login="kaicenat", display_name="KaiCenat", acl_based=True, pinned=True)
        # Kai is streaming Just Chatting (509658) instead of GTA 6 game ID
        ch_kai._stream = Stream(
            ch_kai,
            id=111,
            game={"id": "509658", "name": "Just Chatting"},
            viewers=300000,
            title="GTA 6 SOON",
        )
        self.assertTrue(camp.can_earn(ch_kai), "ACL streamer must be earnable even in Just Chatting")

        # Non-ACL campaign verified via AvailableDrops GQL on channel
        payload_open = _make_campaign_payload(
            cid="camp_gta_open",
            name="GTA 6 Open Drop",
            game_id="999999",
            game_name="Grand Theft Auto VI",
            acl_channels=None,
        )
        camp_open = DropsCampaign(worker, payload_open, {}, verified_channel_ids={777001})
        self.assertTrue(camp_open.can_earn(ch_kai), "AvailableDrops-verified channel must earn even in Just Chatting")

        # Unverified random channel streaming Just Chatting must NOT match GTA 6 campaign
        ch_other = Channel(worker, id=888002, login="randomguy")
        ch_other._stream = Stream(ch_other, id=222, game={"id": "509658", "name": "Just Chatting"}, viewers=50, title="hi")
        self.assertFalse(camp_open.can_earn(ch_other))

    def test_radar_auto_promotes_gta6_campaign(self) -> None:
        payload = _make_campaign_payload(
            cid="camp_gta6",
            name="Grand Theft Auto VI Premiere Drops",
            game_id="1234567",
            game_name="Grand Theft Auto VI",
        )
        promoted = self.radar.promote_game_if_matches(payload)
        self.assertEqual(promoted, "Grand Theft Auto VI")
        self.assertIn("Grand Theft Auto VI", self.radar.auto_promoted_games)

        worker = self._make_worker("acc_prio")
        eff = worker.effective_priority_games()
        self.assertEqual(eff[0], "Grand Theft Auto VI")

    def test_orchestrator_import_and_isolated_multi_workers(self) -> None:
        async def run() -> None:
            legacy_dir = self.tmp_path / "TwitchDropsMiner"
            (legacy_dir / "profiles" / "second").mkdir(parents=True)
            (legacy_dir / "cookies.jar").write_text("dummy_main_cookie", encoding="utf8")
            (legacy_dir / "profiles" / "second" / "cookies.jar").write_text("dummy_second_cookie", encoding="utf8")

            orch = MultiAccountOrchestrator(profiles_dir=self.profiles_dir, global_settings=self.gs)
            imported = orch.import_from_legacy_miner(legacy_dir)
            self.assertEqual(set(imported), {"main_account", "second"})
            orch.add_profile("third_acc", proxy="socks5://127.0.0.1:9050")

            self.assertEqual(len(orch.workers), 3)
            w1 = orch.workers["main_account"]
            w2 = orch.workers["second"]
            w3 = orch.workers["third_acc"]

            # Verify unique device fingerprints and isolated file paths
            self.assertEqual(len({w1.config.device_id, w2.config.device_id, w3.config.device_id}), 3)
            self.assertNotEqual(w1.cookies_path, w2.cookies_path)
            self.assertEqual(w3.proxy_url, "socks5://127.0.0.1:9050")
            self.assertEqual(w1.cookies_path.read_text(encoding="utf8"), "dummy_main_cookie")
            self.assertEqual(w2.cookies_path.read_text(encoding="utf8"), "dummy_second_cookie")

            # Test aiohttp sessions can initialize concurrently in a single asyncio loop & close cleanly
            s1, s2 = await asyncio.gather(w1.get_session(), w2.get_session())
            self.assertIsNot(s1, s2)
            await orch.stop_all()

        asyncio.run(run())

    def test_hybrid_watch_spade_and_gui_smoke(self) -> None:
        async def run() -> None:
            worker = self._make_worker("acc_hybrid")
            ch = Channel(worker, id=777, login="kaicenat", display_name="KaiCenat", pinned=True)
            stream = Stream(
                ch,
                id=888,
                game={"id": "509658", "name": "Just Chatting"},
                viewers=100,
                title="test",
            )
            stream._stream_url = URLType("https://video-weaver.example.com/playlist.m3u8")
            ch._stream = stream
            self.radar.set_cached_spade_url("kaicenat", URLType("https://spade.twitch.tv/track"))

            methods_called: list[tuple[str, str]] = []
            playlist_body = "#EXTM3U\n#EXTINF:2.0,\nhttps://cdn.example.com/last.ts\n"

            def fake_request(method: str, url, **_kwargs):
                methods_called.append((method, str(url)))
                if method == "GET":
                    return _DummyResponse(200, playlist_body)
                if method == "HEAD":
                    return _DummyResponse(200, "")
                return _DummyResponse(204, "")

            worker.request = fake_request  # type: ignore[method-assign]
            ok = await ch.send_watch_hybrid(send_spade=True)
            self.assertTrue(ok)
            self.assertIn(("HEAD", "https://cdn.example.com/last.ts"), methods_called)
            self.assertIn(("POST", "https://spade.twitch.tv/track"), methods_called)

        asyncio.run(run())

        from gui_dashboard import (
            AsyncLoopThread,
            DesktopBridge,
            MultiMinerDashboard,
            UI_HTML_PATH,
        )
        from tray_win32 import Win32TrayController

        self.assertTrue(UI_HTML_PATH.exists(), f"Missing UI file: {UI_HTML_PATH}")
        html = UI_HTML_PATH.read_text(encoding="utf-8")
        for token in ("#0f0f10", "#141415", "#19191b", "cubic-bezier(0.22, 1, 0.36, 1)"):
            self.assertIn(token, html)
        self.assertNotIn("linear-gradient", html)
        for slop_emoji in ("🔥", "✨", "🚀", "💎", "🎮", "✅", "🟢"):
            self.assertNotIn(slop_emoji, html)

        orch = MultiAccountOrchestrator(profiles_dir=self.profiles_dir, global_settings=self.gs)
        orch.add_profile("gui_test_acc")
        loop_thread = AsyncLoopThread()
        loop_thread.start()
        dash = MultiMinerDashboard(orch, loop_thread, start_in_tray=True)
        self.assertIsInstance(dash.bridge, DesktopBridge)
        self.assertIsInstance(dash.tray, Win32TrayController)

        snap = dash.bridge.get_state()
        by_name = {a["name"]: a for a in snap["accounts"]}
        self.assertIn("gui_test_acc", by_name)

        res_proxy = dash.bridge.update_proxy("gui_test_acc", "http://127.0.0.1:8888")
        self.assertTrue(res_proxy["ok"])
        updated_by_name = {
            a["name"]: a for a in dash.bridge.get_state()["accounts"]
        }
        self.assertEqual(updated_by_name["gui_test_acc"]["proxy"], "http://127.0.0.1:8888")

        with patch.object(orch.global_settings, "save"):
            res_save = dash.bridge.save_settings(
                {
                    "pinned_streamers": ["kaicenat", "tarik"],
                    "auto_priority_keywords": ["Grand Theft Auto VI", "GTA"],
                    "priority_games": ["Grand Theft Auto VI"],
                    "exclude_games": [],
                    "priority_mode": "ENDING_SOONEST",
                    "connection_quality": 2,
                    "radar_interval_sec": 60,
                    "hybrid_watch_spade": True,
                    "standby_on_pinned": True,
                    "enable_badges_emotes": False,
                    "available_drops_check": True,
                }
            )
        self.assertTrue(res_save["ok"])
        self.assertEqual(orch.global_settings.pinned_streamers, ["kaicenat", "tarik"])
        self.assertEqual(orch.global_settings.connection_quality, 2)

        dash._tray_active = True
        self.assertTrue(dash._on_window_closing())
        dash._on_exit()

    def test_fallback_dns_resolver_packet_and_udp_fallback(self) -> None:
        import socket
        import struct
        import aiohttp
        from utils import (
            FallbackDNSResolver,
            _build_dns_query,
            _parse_dns_a_response,
            make_connector,
        )

        pkt = _build_dns_query("gql.twitch.tv", 0x1234)
        self.assertTrue(pkt.startswith(struct.pack("!HHHHHH", 0x1234, 0x0100, 1, 0, 0, 0)))

        # Construct a compressed DNS response packet (QNAME + pointer 0xC00C -> A 146.75.118.214, TTL 60)
        question_wire = pkt[12:]
        resp_header = struct.pack("!HHHHHH", 0x1234, 0x8180, 1, 1, 0, 0)
        answer_rr = (
            b"\xc0\x0c"
            + struct.pack("!HHIH", 1, 1, 60, 4)
            + socket.inet_aton("146.75.118.214")
        )
        ips, ttl = _parse_dns_a_response(resp_header + question_wire + answer_rr, 0x1234)
        self.assertEqual(ips, ["146.75.118.214"])
        self.assertEqual(ttl, 60)

        async def run() -> None:
            resolver = FallbackDNSResolver()
            resolver._aiodns_unavailable = True  # Force UDP fallback path in test
            ip_literal = await resolver.resolve("127.0.0.1", 443)
            self.assertEqual(ip_literal[0]["host"], "127.0.0.1")

            local_res = await resolver.resolve("localhost", 80)
            self.assertEqual(local_res[0]["host"], "127.0.0.1")

            with patch("utils._udp_dns_lookup", return_value=(["146.75.118.214"], 90)) as mocked_udp:
                first = await resolver.resolve("gql.twitch.tv", 443)
                second = await resolver.resolve("gql.twitch.tv", 443)
                self.assertEqual(first[0]["host"], "146.75.118.214")
                self.assertEqual(second[0]["host"], "146.75.118.214")
                self.assertEqual(mocked_udp.call_count, 1)  # Served from TTL cache on second lookup

            await resolver.close()
            conn = make_connector()
            self.assertIsInstance(conn, aiohttp.TCPConnector)
            await conn.close()

        asyncio.run(run())

    def test_cp1251_safe_logging_and_claimed_history_bridge(self) -> None:
        import io
        import logging
        from gui_dashboard import AsyncLoopThread, MultiMinerDashboard
        from main import _SafeStreamHandler

        raw_buf = io.BytesIO()
        cp1251_stream = io.TextIOWrapper(raw_buf, encoding="cp1251", errors="strict")
        handler = _SafeStreamHandler(cp1251_stream)
        handler.setFormatter(logging.Formatter("{message}", style="{"))
        rec = logging.LogRecord(
            name="TwitchMultiMiner.worker",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="[main_account] 🎁 Frostbite ❄️ Drop claimed",
            args=(),
            exc_info=None,
        )
        handler.emit(rec)
        text_written = raw_buf.getvalue().decode("cp1251")
        self.assertIn("Frostbite", text_written)

        orch = MultiAccountOrchestrator(profiles_dir=self.profiles_dir, global_settings=self.gs)
        w = orch.add_profile("claim_acc")
        fake_drop = MagicMock()
        fake_drop.campaign.game.name = "FragPunk"
        fake_drop.campaign.name = "FragPunk Launch"
        fake_drop.rewards_text.return_value = "Molotov Cocktail"
        fake_drop.primary_image_url = "https://static-cdn.jtvnw.net/fragpunk-molotov.png"
        w.record_claimed_drop(fake_drop)

        loop_thread = AsyncLoopThread()
        loop_thread.start()
        dash = MultiMinerDashboard(orch, loop_thread, start_in_tray=True)
        dash.bridge.image_cache.prefetch = lambda _urls: None  # type: ignore[method-assign]
        state = dash.bridge.get_state()
        self.assertEqual(len(state["claimed_history"]), 1)
        self.assertEqual(state["claimed_history"][0]["drop_name"], "Molotov Cocktail")
        self.assertEqual(state["claimed_history"][0]["game"], "FragPunk")
        self.assertEqual(
            state["claimed_history"][0]["image_url"],
            "https://static-cdn.jtvnw.net/fragpunk-molotov.png",
        )
        dash._on_exit()

    def test_campaign_item_images_and_cache(self) -> None:
        import hashlib
        from gui_dashboard import (
            IMAGE_CACHE_DIR,
            AsyncLoopThread,
            ImageCacheManager,
            MultiMinerDashboard,
            _normalize_image_url,
        )

        worker = self._make_worker("img_acc")
        payload = _make_campaign_payload(
            cid="camp_img_1",
            name="GTA 6 Art Campaign",
            game_id="999999",
            game_name="Grand Theft Auto VI",
        )
        payload["game"]["boxArtURL"] = (
            "https://static-cdn.jtvnw.net/ttv-boxart/999-{width}x{height}.jpg"
        )
        payload["timeBasedDrops"][0]["benefitEdges"] = [
            {
                "benefit": {
                    "id": "ben_1",
                    "name": "Vice City Jacket",
                    "distributionType": "DIRECT_ENTITLEMENT",
                    "imageAssetURL": "https://static-cdn.jtvnw.net/twitch-drops/jacket_{width}x{height}.png",
                }
            },
            {
                "benefit": {
                    "id": "ben_2",
                    "name": "Neon Helmet",
                    "distributionType": "IN_GAME",
                    "imageURL": "https://static-cdn.jtvnw.net/twitch-drops/helmet.webp",
                }
            },
        ]
        drop2_payload = dict(payload["timeBasedDrops"][0])
        drop2_payload["id"] = "drop_2"
        drop2_payload["name"] = "GTA 6 Box Art Emblem"
        drop2_payload["benefitEdges"] = [
            {
                "benefit": {
                    "id": "ben_3",
                    "name": "Emblem Only",
                    "distributionType": "DIRECT_ENTITLEMENT",
                    "imageAssetURL": "",
                }
            }
        ]
        payload["timeBasedDrops"].append(drop2_payload)

        camp = DropsCampaign(worker, payload, {})
        d1 = camp.timed_drops["drop_1"]
        d2 = camp.timed_drops["drop_2"]
        self.assertEqual(
            str(d1.benefits[0].image_url),
            "https://static-cdn.jtvnw.net/twitch-drops/jacket_{width}x{height}.png",
        )
        self.assertEqual(
            str(d1.benefits[1].image_url),
            "https://static-cdn.jtvnw.net/twitch-drops/helmet.webp",
        )
        self.assertEqual(
            d1.primary_image_url,
            "https://static-cdn.jtvnw.net/twitch-drops/jacket_{width}x{height}.png",
        )
        self.assertEqual(d2.primary_image_url, str(camp.image_url))

        norm_box = _normalize_image_url(payload["game"]["boxArtURL"])
        self.assertEqual(
            norm_box,
            "https://static-cdn.jtvnw.net/ttv-boxart/999-144x192.jpg",
        )

        loop_thread = AsyncLoopThread()
        loop_thread.start()
        dash = None
        try:
            cache_dir = self.tmp_path / "img_cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            img_url = "https://static-cdn.jtvnw.net/twitch-drops/jacket.png"
            digest = hashlib.sha256(img_url.encode("utf-8")).hexdigest()[:24]
            cached_file = cache_dir / f"{digest}.png"
            cached_file.write_bytes(b"\x89PNG\r\n\x1a\n")

            mgr = ImageCacheManager(loop_thread, cache_dir=cache_dir)
            resolved_hit, source_hit = mgr.resolve(img_url)
            self.assertTrue(resolved_hit.startswith("file:///"))
            self.assertTrue(resolved_hit.endswith(f"{digest}.png"))
            self.assertEqual(source_hit, img_url)

            # Verify default IMAGE_CACHE_DIR serves WebView2-compatible relative URI
            rel_test_url = "https://static-cdn.jtvnw.net/twitch-drops/webview_test_unique_asset.png"
            rel_digest = hashlib.sha256(rel_test_url.encode("utf-8")).hexdigest()[:24]
            IMAGE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            rel_file = IMAGE_CACHE_DIR / f"{rel_digest}.png"
            rel_file.write_bytes(b"\x89PNG\r\n\x1a\n")
            try:
                ui_mgr = ImageCacheManager(loop_thread, cache_dir=IMAGE_CACHE_DIR)
                rel_disp, rel_orig = ui_mgr.resolve(rel_test_url)
                self.assertEqual(rel_disp, f"cache/images/{rel_digest}.png")
                self.assertEqual(rel_orig, rel_test_url)
            finally:
                rel_file.unlink(missing_ok=True)

            miss_url = "https://static-cdn.jtvnw.net/twitch-drops/uncached_{width}x{height}.jpg"
            resolved_miss, source_miss = mgr.resolve(miss_url)
            self.assertEqual(
                resolved_miss,
                "https://static-cdn.jtvnw.net/twitch-drops/uncached_144x192.jpg",
            )
            self.assertEqual(source_miss, resolved_miss)

            orch = MultiAccountOrchestrator(profiles_dir=self.profiles_dir, global_settings=self.gs)
            w = orch.add_profile("bridge_img_acc")
            w.inventory.append(camp)
            w.record_claimed_drop(d1)

            dash = MultiMinerDashboard(orch, loop_thread, start_in_tray=True)
            dash.bridge.image_cache = mgr
            dash.bridge.image_cache.prefetch = lambda _urls: None  # type: ignore[method-assign]
            state = dash.bridge.get_state()

            self.assertEqual(len(state["campaigns"]), 1)
            c_state = state["campaigns"][0]
            self.assertIn("image_url", c_state)
            self.assertEqual(
                c_state["image_url"],
                "https://static-cdn.jtvnw.net/ttv-boxart/999-144x192.jpg",
            )
            self.assertEqual(len(c_state["drops"]), 2)
            ds1 = c_state["drops"][0]
            self.assertEqual(
                ds1["image_url"],
                "https://static-cdn.jtvnw.net/twitch-drops/jacket_144x192.png",
            )
            self.assertTrue(ds1["rewards_text"])
            self.assertEqual(len(ds1["benefits"]), 2)
            self.assertEqual(
                ds1["benefits"][1]["image_url"],
                "https://static-cdn.jtvnw.net/twitch-drops/helmet.webp",
            )
            ds2 = c_state["drops"][1]
            self.assertEqual(
                ds2["image_url"],
                "https://static-cdn.jtvnw.net/ttv-boxart/999-144x192.jpg",
            )
            self.assertEqual(len(state["claimed_history"]), 1)
            self.assertEqual(
                state["claimed_history"][0]["image_url"],
                "https://static-cdn.jtvnw.net/twitch-drops/jacket_144x192.png",
            )

            # Verify favorite toggling + persistence + worker priority boost
            tmp_gs_path = self.tmp_path / "global_settings.json"
            self.gs.save = lambda path=tmp_gs_path: GlobalSettings.save(self.gs, tmp_gs_path)  # type: ignore[method-assign]
            res_on = dash.bridge.toggle_favorite_campaign("camp_img_1")
            self.assertTrue(res_on["ok"])
            self.assertTrue(res_on["is_favorite"])
            reloaded = GlobalSettings.load(tmp_gs_path)
            self.assertEqual(reloaded.favorite_campaigns, ["camp_img_1"])
            self.assertTrue(dash.bridge.get_state()["campaigns"][0]["is_favorite"])

            w.inventory.append(camp)
            self.assertIn("Grand Theft Auto VI", w.effective_priority_games())

            res_off = dash.bridge.toggle_favorite_campaign("camp_img_1")
            self.assertTrue(res_off["ok"])
            self.assertFalse(res_off["is_favorite"])
            reloaded2 = GlobalSettings.load(tmp_gs_path)
            self.assertEqual(reloaded2.favorite_campaigns, [])
            self.assertFalse(dash.bridge.get_state()["campaigns"][0]["is_favorite"])

            dash._on_exit()
            dash = None
        finally:
            if dash is not None:
                dash._on_exit()
            else:
                loop_thread.stop()

    def test_radar_presets_and_pc_games_scan(self) -> None:
        from gui_dashboard import AsyncLoopThread, DesktopBridge
        from radar import scan_installed_pc_games

        with patch("radar._scan_steam_games", return_value=["FragPunk", "Overcooked! 2"]), patch(
            "radar._scan_epic_games", return_value=["Fortnite"]
        ), patch("radar._scan_riot_games", return_value=["VALORANT"]), patch(
            "radar._scan_registry_games", return_value=[]
        ):
            scanned = scan_installed_pc_games()
            self.assertEqual(scanned, ["FragPunk", "Overcooked! 2", "Fortnite", "VALORANT"])

            loop_thread = AsyncLoopThread()
            try:
                orch = MultiAccountOrchestrator(profiles_dir=self.profiles_dir, global_settings=self.gs)
                tmp_gs_path = self.tmp_path / "global_settings_presets.json"
                self.gs.save = lambda path=tmp_gs_path: GlobalSettings.save(self.gs, tmp_gs_path)  # type: ignore[method-assign]

                orch.radar._catalog_campaigns = {
                    "c_rust": {"id": "c_rust", "game": {"name": "Rust"}},
                    "c_val": {"id": "c_val", "game": {"name": "VALORANT"}},
                }

                worker = orch.add_profile("preset_acc")
                now = datetime.now(timezone.utc)
                camp_pc = DropsCampaign(
                    worker,
                    {
                        "id": "camp_fp",
                        "name": "FragPunk Drops",
                        "status": "ACTIVE",
                        "startAt": (now - timedelta(hours=1)).isoformat(),
                        "endAt": (now + timedelta(hours=3)).isoformat(),
                        "self": {"isAccountConnected": True},
                        "allow": {"isEnabled": False, "channels": []},
                        "game": {"id": "555", "name": "FragPunk", "displayName": "FragPunk"},
                        "timeBasedDrops": [
                            {
                                "id": "d_fp",
                                "name": "FragPunk Skin",
                                "requiredMinutesWatched": 30,
                                "benefitEdges": [],
                            }
                        ],
                    },
                    {},
                )
                camp_other = DropsCampaign(
                    worker,
                    {
                        "id": "camp_other",
                        "name": "Apex Drops",
                        "status": "ACTIVE",
                        "startAt": (now - timedelta(hours=1)).isoformat(),
                        "endAt": (now + timedelta(hours=4)).isoformat(),
                        "self": {"isAccountConnected": True},
                        "allow": {"isEnabled": False, "channels": []},
                        "game": {"id": "777", "name": "Apex Legends", "displayName": "Apex Legends"},
                        "timeBasedDrops": [
                            {
                                "id": "d_apx",
                                "name": "Apex Charm",
                                "requiredMinutesWatched": 30,
                                "benefitEdges": [],
                            }
                        ],
                    },
                    {},
                )
                worker.inventory.extend([camp_pc, camp_other])

                pc_list = orch.radar.get_installed_pc_games(force=True, known_games=["FragPunk", "VALORANT"])
                self.assertEqual(pc_list[0], "VALORANT")
                self.assertIn("FragPunk", pc_list)

                bridge = DesktopBridge(orch, loop_thread)

                res_all = bridge.apply_radar_preset("all_campaigns")
                self.assertTrue(res_all["ok"])
                self.assertEqual(res_all["preset"], "all_campaigns")
                self.assertIn("Apex Legends", self.gs.priority_games)
                self.assertIn("Rust", self.gs.priority_games)
                self.assertEqual(self.gs.priority_mode, "ENDING_SOONEST")
                self.assertIn("Apex Legends", worker.effective_priority_games())

                res_pc = bridge.apply_radar_preset("pc_games")
                self.assertTrue(res_pc["ok"])
                self.assertEqual(res_pc["preset"], "pc_games")
                self.assertEqual(self.gs.priority_mode, "PRIORITY_ONLY")
                self.assertIn("VALORANT", self.gs.priority_games)
                self.assertIn("FragPunk", self.gs.priority_games)
                self.assertNotIn("Apex Legends", self.gs.priority_games)

                reloaded = GlobalSettings.load(tmp_gs_path)
                self.assertEqual(reloaded.radar_preset, "pc_games")
                self.assertEqual(reloaded.priority_mode, "PRIORITY_ONLY")

                state = bridge.get_state()
                self.assertEqual(state["radar"]["preset"], "pc_games")
                self.assertIn("Apex Legends", state["radar"]["available_campaign_games"])
                self.assertIn("FragPunk", state["radar"]["pc_games"])

                res_def = bridge.apply_radar_preset("default")
                self.assertTrue(res_def["ok"])
                self.assertEqual(self.gs.radar_preset, "default")
                self.assertEqual(self.gs.priority_games, list(DEFAULT_PRIORITY_GAMES))
                self.assertEqual(self.gs.priority_mode, "ENDING_SOONEST")
            finally:
                loop_thread.stop()

    def test_worker_auto_restart_after_crash(self) -> None:
        async def run() -> None:
            worker = self._make_worker("crash_acc")
            attempts = 0
            sleep_delays: list[float] = []

            async def fake_run() -> None:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise RuntimeError("transient network boom")
                worker.stop_nowait()

            async def fast_sleep(seconds: float) -> None:
                sleep_delays.append(seconds)

            worker._run = fake_run  # type: ignore[method-assign]
            worker.sleep_or_stop = fast_sleep  # type: ignore[method-assign]
            await worker.run()
            self.assertEqual(attempts, 2, "Worker must auto-restart after an unexpected crash")
            self.assertTrue(sleep_delays and sleep_delays[0] > 0)

        asyncio.run(run())

    def test_dpapi_cookie_protection_and_legacy_migration(self) -> None:
        import aiohttp
        from yarl import URL
        from auth_transport import (
            load_auth_token_from_jar,
            protect_secret,
            save_cookie_jar_protected,
            unprotect_secret,
        )

        raw = "oauth_live_secret_token_987654"
        enc = protect_secret(raw)
        self.assertNotEqual(enc, raw)
        self.assertEqual(unprotect_secret(enc), raw)

        async def run() -> None:
            url = URL("https://www.twitch.tv")
            cookies_path = self.tmp_path / "protected_cookies.jar"
            jar = aiohttp.CookieJar()
            save_cookie_jar_protected(jar, cookies_path, url, raw)

            disk_bytes = cookies_path.read_bytes()
            self.assertNotIn(raw.encode("utf8"), disk_bytes, "Plaintext auth-token must never be written to cookies.jar")

            jar_reloaded = aiohttp.CookieJar()
            self.assertEqual(load_auth_token_from_jar(jar_reloaded, cookies_path, url), raw)

            # Verify legacy plaintext cookies.jar auto-migrates to protected form
            legacy_path = self.tmp_path / "legacy_cookies.jar"
            legacy_jar = aiohttp.CookieJar()
            legacy_jar.update_cookies({"auth-token": raw, "persistent": "12345"}, url)
            legacy_jar.save(legacy_path)
            self.assertIn(raw.encode("utf8"), legacy_path.read_bytes())

            jar_mig = aiohttp.CookieJar()
            loaded = load_auth_token_from_jar(jar_mig, legacy_path, url)
            self.assertEqual(loaded, raw)
            self.assertNotIn(raw.encode("utf8"), legacy_path.read_bytes())

        asyncio.run(run())

    def test_egress_ip_shared_rate_limiter_and_safe_gql_error_path(self) -> None:
        from auth_transport import _nullify_gql_error_path, normalize_egress_key

        self.assertEqual(normalize_egress_key(""), "direct")
        self.assertEqual(
            normalize_egress_key("http://userA:passA@10.0.0.5:8080"),
            "http://10.0.0.5:8080",
        )

        w1 = self._make_worker("acc_direct_1")
        w2 = self._make_worker("acc_direct_2")
        self.assertIs(w1.get_gql_limiter(), w2.get_gql_limiter())

        w3_cfg = AccountProfileConfig.load("acc_p1", self.profiles_dir)
        w3_cfg.proxy = "http://u1:p1@192.168.10.2:3128"
        w3 = AccountWorker(w3_cfg, self.gs, self.radar, profiles_dir=self.profiles_dir)

        w4_cfg = AccountProfileConfig.load("acc_p2", self.profiles_dir)
        w4_cfg.proxy = "http://u2:p2@192.168.10.2:3128"
        w4 = AccountWorker(w4_cfg, self.gs, self.radar, profiles_dir=self.profiles_dir)

        self.assertIs(w3.get_gql_limiter(), w4.get_gql_limiter())
        self.assertIsNot(w1.get_gql_limiter(), w3.get_gql_limiter())

        # Ensure _nullify_gql_error_path handles data=None, missing keys, and list indices
        _nullify_gql_error_path({"data": None}, ["user", "dropCampaign"])
        _nullify_gql_error_path({"data": {"user": None}}, ["user", "dropCampaign"])
        item_list: dict = {"data": {"items": [{"x": 1}, {"x": 2}]}}
        _nullify_gql_error_path(item_list, ["items", 1, "x"])
        self.assertIsNone(item_list["data"]["items"][1]["x"])

    def test_device_flow_slow_down_and_expired_token(self) -> None:
        from contextlib import asynccontextmanager
        import json

        async def run() -> None:
            worker = self._make_worker("dev_flow")
            sleeps: list[float] = []
            token_poll_count = 0

            async def fake_sleep(sec: float) -> None:
                sleeps.append(sec)

            @asynccontextmanager
            async def fake_request(method: str, url, **_kwargs):
                nonlocal token_poll_count
                u = str(url)
                if u.endswith("/device"):
                    yield _DummyResponse(
                        200,
                        json.dumps(
                            {
                                "device_code": f"dev_{len(sleeps)}",
                                "user_code": "ABCD1234",
                                "interval": 5,
                                "verification_uri": "https://www.twitch.tv/activate",
                                "expires_in": 600,
                            }
                        ),
                    )
                    return
                token_poll_count += 1
                if token_poll_count == 1:
                    yield _DummyResponse(400, json.dumps({"status": 400, "error": "slow_down"}))
                elif token_poll_count == 2:
                    yield _DummyResponse(400, json.dumps({"status": 400, "message": "authorization_pending"}))
                elif token_poll_count == 3:
                    yield _DummyResponse(400, json.dumps({"status": 400, "error": "expired_token"}))
                else:
                    yield _DummyResponse(200, json.dumps({"access_token": "fresh_tok_after_restart"}))

            worker.sleep_or_stop = fake_sleep  # type: ignore[method-assign]
            worker.request = fake_request  # type: ignore[method-assign]

            token = await worker._auth_state._oauth_device_login()
            self.assertEqual(token, "fresh_tok_after_restart")
            # Poll 1: 5s -> slow_down -> Poll 2: 10s -> pending -> Poll 3: 10s -> expired -> new code -> Poll 4: 5s
            self.assertEqual(sleeps, [5, 10, 10, 5])

        asyncio.run(run())

    def test_pc_games_preset_local_only_never_queries_non_campaign_games(self) -> None:
        async def run() -> None:
            worker = self._make_worker("pc_privacy")
            camp_val = DropsCampaign(
                worker,
                _make_campaign_payload(
                    cid="camp_val_priv",
                    name="VALORANT Drops",
                    game_id="516575",
                    game_name="VALORANT",
                    required_minutes=15,
                ),
                {},
            )
            worker.inventory = [camp_val]
            self.gs.radar_preset = "pc_games"
            self.gs.priority_games = ["SecretLocalIndieGame", "PrivateToolGame", "VALORANT"]
            self.gs.priority_mode = "PRIORITY_ONLY"

            queried_slugs: list[str] = []

            async def fake_get_streams(game: Game, *, limit: int = 20, drops_enabled: bool = True):
                queried_slugs.append(game.slug)
                return []

            worker.get_live_streams = fake_get_streams  # type: ignore[method-assign]
            worker._compute_wanted_games()
            await worker._fetch_channels()

            self.assertEqual([g.name for g in worker.wanted_games], ["VALORANT"])
            self.assertEqual(queried_slugs, ["valorant"])
            self.assertNotIn("secretlocalindiegame", queried_slugs)
            self.assertNotIn("privatetoolgame", queried_slugs)

        asyncio.run(run())

    def test_remove_profile_blocks_path_traversal(self) -> None:
        async def run() -> None:
            outside_dir = self.tmp_path / "outside_secret"
            outside_dir.mkdir(parents=True, exist_ok=True)
            marker = outside_dir / "keep.txt"
            marker.write_text("safe", encoding="utf8")

            orch = MultiAccountOrchestrator(
                profiles_dir=self.tmp_path / "profiles",
                global_settings=self.gs,
            )
            for bad_name in ("../outside_secret", "..\\outside_secret", "../etc", "."):
                with self.assertRaises(ValueError):
                    await orch.remove_profile(bad_name)
            self.assertTrue(marker.is_file())

        asyncio.run(run())

    def test_proxy_credentials_protected_on_disk_and_legacy_migrated(self) -> None:
        import json

        base_dir = self.tmp_path / "profiles_proxy"
        secret_proxy = "http://myuser:s3cretPass!@1.2.3.4:8080"
        cfg = AccountProfileConfig(name="proxy_acc", proxy=secret_proxy)
        cfg.ensure_identity()
        cfg.save(base_dir)

        disk_text = AccountProfileConfig.config_path("proxy_acc", base_dir).read_text(encoding="utf8")
        self.assertNotIn("s3cretPass!", disk_text)

        reloaded = AccountProfileConfig.load("proxy_acc", base_dir)
        self.assertEqual(reloaded.proxy, secret_proxy)

        # Legacy plaintext account.json auto-migrates on load
        legacy_path = AccountProfileConfig.config_path("legacy_proxy", base_dir)
        legacy_path.parent.mkdir(parents=True, exist_ok=True)
        legacy_path.write_text(
            json.dumps({"name": "legacy_proxy", "proxy": secret_proxy}),
            encoding="utf8",
        )
        migrated = AccountProfileConfig.load("legacy_proxy", base_dir)
        self.assertEqual(migrated.proxy, secret_proxy)
        self.assertNotIn("s3cretPass!", legacy_path.read_text(encoding="utf8"))

    def test_orchestrator_public_auth_and_profile_file_logger(self) -> None:
        async def run() -> None:
            orch = MultiAccountOrchestrator(
                profiles_dir=self.tmp_path / "profiles_log",
                global_settings=self.gs,
            )
            worker = self._make_worker("log_acc")
            self.assertTrue(worker.is_authenticated)

            worker._setup_file_logger()
            worker.log("hello_standard_logger")
            worker._close_file_logger()
            self.assertIn("hello_standard_logger", worker.log_path.read_text(encoding="utf8"))

            calls = 0

            def good_cb() -> None:
                nonlocal calls
                calls += 1

            def bad_cb() -> None:
                raise RuntimeError("boom callback")

            orch.add_change_listener(bad_cb)
            orch.add_change_listener(good_cb)
            with self.assertLogs("TwitchMultiMiner.orchestrator", level="ERROR"):
                orch._emit_change()
            self.assertEqual(calls, 1)

        asyncio.run(run())

    def test_exclusive_campaign_ignores_other_campaigns_and_waits_for_channels(self) -> None:
        from constants import State

        async def run() -> None:
            worker = self._make_worker("exclusive_acc")
            camp_a = DropsCampaign(
                worker,
                _make_campaign_payload(
                    cid="camp_a",
                    name="Other Drops",
                    game_id="1001",
                    game_name="OtherGame",
                    required_minutes=30,
                ),
                {},
            )
            camp_b = DropsCampaign(
                worker,
                _make_campaign_payload(
                    cid="camp_b",
                    name="Target Mission",
                    game_id="1002",
                    game_name="TargetGame",
                    required_minutes=60,
                ),
                {},
            )
            worker.inventory = [camp_a, camp_b]
            worker._campaigns = {"camp_a": camp_a, "camp_b": camp_b}

            self.gs.standby_on_pinned = True
            self.gs.exclusive_campaign_id = "camp_b"

            # Add an online pinned streamer to radar to ensure exclusive mode ignores standby
            st = worker.radar.pinned_streams.setdefault("some_pinned", type(next(iter(worker.radar.pinned_streams.values()), object())) if worker.radar.pinned_streams else None)  # type: ignore[arg-type]
            if st is None:
                from radar import PinnedStreamState

                st = PinnedStreamState(
                    login="some_pinned",
                    channel_id=777,
                    display_name="SomePinned",
                    online=True,
                    raw_channel_data={"stream": {"id": "77701", "viewersCount": 5000, "game": {"id": "1001", "name": "OtherGame"}}},
                )
                worker.radar.pinned_streams["some_pinned"] = st

            queried_games: list[str] = []

            async def fake_get_streams(game: Game, *, limit: int = 20, drops_enabled: bool = True):
                queried_games.append(game.name)
                return []

            worker.get_live_streams = fake_get_streams  # type: ignore[method-assign]
            worker._compute_wanted_games()
            self.assertEqual([g.name for g in worker.wanted_games], ["TargetGame"])

            await worker._fetch_channels()
            self.assertEqual(queried_games, ["TargetGame"])
            self.assertNotIn(777, worker.channels)

            # Even if a live channel for OtherGame exists, exclusive mode must refuse it and wait in IDLE
            ch_other = Channel(worker, id=888, login="other_live", display_name="OtherLive")
            ch_other._stream = Stream(
                ch_other,
                id=88801,
                title="OtherGame Drops",
                game={"id": "1001", "name": "OtherGame"},
                viewers=100,
            )
            ch_other._stream.drops_enabled = True
            worker.channels[888] = ch_other
            self.assertFalse(worker.can_watch(ch_other))

            worker._switch_channel()
            self.assertIsNone(worker.watching_channel.get_with_default(None))
            self.assertEqual(worker._state, State.IDLE)

            # Once a channel for TargetGame goes live, worker selects it and picks camp_b
            ch_target = Channel(worker, id=999, login="target_live", display_name="TargetLive")
            ch_target._stream = Stream(
                ch_target,
                id=99901,
                title="Target Mission Drops",
                game={"id": "1002", "name": "TargetGame"},
                viewers=250,
            )
            ch_target._stream.drops_enabled = True
            worker.channels[999] = ch_target
            self.assertTrue(worker.can_watch(ch_target))
            worker._switch_channel()
            self.assertEqual(worker.watching_channel.get_with_default(None), ch_target)
            active_camp = worker.get_active_campaign(ch_target)
            self.assertIsNotNone(active_camp)
            assert active_camp is not None
            self.assertEqual(active_camp.id, "camp_b")

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main(verbosity=2)
