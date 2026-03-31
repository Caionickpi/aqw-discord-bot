"""
Bot de Discord moderno para AQW usando discord.py 2.0+.

Dependencias recomendadas:
    pip install -U discord.py requests beautifulsoup4 selenium

Observacoes importantes:
1. Insira o token do bot na variavel TOKEN no final do arquivo
   ou defina a env var DISCORD_TOKEN.
2. Para a captura da imagem do personagem, o codigo usa
   Selenium + Chrome/Chromium em modo headless.
3. A imagem do personagem nao vem pronta no HTML da Char Page.
   Ela e renderizada no elemento <ruffle-embed>, entao o bot
   precisa abrir a pagina num navegador headless e tirar
   um screenshot desse bloco.
4. A vinculacao Discord -> AQW e persistida em SQLite no arquivo
   aqw_links.db ao lado deste script.
"""

from __future__ import annotations

import asyncio
import base64
import html
import io
import logging
import os
import re
import sqlite3
import tempfile
import time
import uuid
from collections import Counter, OrderedDict
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Optional
from urllib.parse import quote_plus

import discord
import requests
from bs4 import BeautifulSoup, NavigableString, Tag
from discord import app_commands
from discord.ext import commands
from discord.http import Route

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "aqw_links.db"
PROFILE_ART_DIR = APP_DIR / "profile_art"
PROFILE_ART_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("aqw-bot")


class AQWError(Exception):
    """Erro base para falhas relacionadas ao scraping do AQW."""


class AQWCharacterNotFound(AQWError):
    """Disparado quando a Char Page nao existe."""


class AQWCharacterUnavailable(AQWError):
    """Disparado quando a Char Page existe, mas nao pode ser lida."""


@dataclass(slots=True)
class AQWBadge:
    category: str
    title: str
    description: str
    file_name: str
    subcategory: str = ""


@dataclass(slots=True)
class AQWCharacterData:
    ccid: int
    queried_nickname: str
    display_name: str
    title: str
    level: str
    faction: str
    guild: str
    url: str
    equipped: dict[str, str]
    inventory: list[dict]
    badges: list[AQWBadge]
    image_path: Optional[Path] = None


@dataclass(slots=True)
class AccountLink:
    discord_user_id: int
    discord_name: str
    aqw_nickname: str
    created_at: str
    updated_at: str


@dataclass(slots=True)
class ProfileSnapshot:
    discord_user_id: int
    aqw_nickname: str
    level: int
    inventory_total: int
    badge_total: int
    farm_total: int
    ultra_total: int
    class_total: int
    top_class_name: str
    top_class_rank: int
    recorded_at: str
    source: str


@dataclass(frozen=True, slots=True)
class FarmDefinition:
    name: str
    category: str
    terms: tuple[str, ...]
    note: str


@dataclass(slots=True)
class FarmStatus:
    definition: FarmDefinition
    completed: bool
    source: Optional[str] = None
    matched_name: Optional[str] = None


FARM_DEFINITIONS: tuple[FarmDefinition, ...] = (
    FarmDefinition("Legion Revenant", "Classes de farm", ("Legion Revenant",), "Classe endgame de Legion."),
    FarmDefinition("Void Highlord", "Classes de farm", ("Void Highlord",), "Classe de Nulgath muito visada."),
    FarmDefinition("ArchMage", "Classes de farm", ("ArchMage",), "Classe caster endgame."),
    FarmDefinition("Dragon of Time", "Classes de farm", ("Dragon of Time",), "Classe ligada a historia e farms longos."),
    FarmDefinition("Chaos Avenger", "Classes de farm", ("Chaos Avenger",), "Classe ultra/endgame."),
    FarmDefinition("Lord of Order", "Classes de farm", ("Lord of Order",), "Classe de suporte chave."),
    FarmDefinition("LightCaster", "Classes de farm", ("LightCaster",), "Classe iconica baseada em LightMage."),
    FarmDefinition("Yami no Ronin", "Classes de farm", ("Yami no Ronin", "YnR"), "Classe de dodge/meta."),
    FarmDefinition("StoneCrusher", "Classes de farm", ("StoneCrusher",), "Classe utilitaria muito usada."),
    FarmDefinition(
        "Blinding Light of Destiny",
        "Armas e equipamentos",
        ("Blinding Light of Destiny", "BLoD"),
        "Arma de farm classica do AQW.",
    ),
    FarmDefinition(
        "Necrotic Sword of Doom",
        "Armas e equipamentos",
        ("Necrotic Sword of Doom", "NSoD"),
        "Uma das farms mais famosas e longas do jogo.",
    ),
    FarmDefinition(
        "Sepulchure's DoomKnight Armor",
        "Armas e equipamentos",
        ("Sepulchure's DoomKnight Armor", "SDKA"),
        "Armor chave para varias progressões ligadas a Doom.",
    ),
    FarmDefinition(
        "Exalted Apotheosis",
        "Armas e equipamentos",
        ("Exalted Apotheosis",),
        "Arma vinculada a ultras/endgame moderno.",
    ),
    FarmDefinition(
        "Hollowborn DoomKnight",
        "Armas e equipamentos",
        ("Hollowborn DoomKnight",),
        "Conjunto endgame bastante reconhecido.",
    ),
)

ULTRA_DEFINITIONS: tuple[FarmDefinition, ...] = (
    FarmDefinition("Chaos Avenger", "Champion Drakath", ("Chaos Avenger",), "Recompensa marcante ligada a ultra progression."),
    FarmDefinition("Exalted Apotheosis", "Timeinn Ultras", ("Exalted Apotheosis",), "Arma clássica de ultras do Timeinn."),
    FarmDefinition("Arcana Invoker", "Ultra Dage / endgame", ("Arcana Invoker",), "Classe endgame ligada a conteúdo avançado."),
    FarmDefinition("Verus DoomKnight", "Ultra Nulgath / Doom", ("Verus DoomKnight",), "Classe endgame associada a conteúdo ultra."),
    FarmDefinition("Radiant Goddess of War", "Ultra Speaker", ("Radiant Goddess of War",), "Reward icônica associada a ultras recentes."),
    FarmDefinition("Dauntless", "Forge / Ultras", ("Dauntless",), "Enhancement endgame ligada a progressão de ultras e forge."),
    FarmDefinition("Valiance", "Forge / Ultras", ("Valiance",), "Enhancement endgame muito buscada."),
    FarmDefinition("Ravenous", "Forge / Ultras", ("Ravenous",), "Enhancement endgame ligada a conteúdo avançado."),
    FarmDefinition("Elysium", "Forge / Ultras", ("Elysium",), "Enhancement de caster endgame."),
    FarmDefinition("Providence", "Forge / Ultras", ("Providence",), "Enhancement defensiva/endgame."),
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_lookup_token(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def safe_user_display(user: discord.abc.User) -> str:
    return getattr(user, "display_name", None) or user.name


def build_field_value(lines: list[str], fallback: str = "Nenhum dado encontrado.", limit: int = 1024) -> str:
    if not lines:
        return fallback

    selected: list[str] = []
    current = 0
    total = len(lines)

    for index, line in enumerate(lines):
        addition = len(line) + (1 if selected else 0)
        if current + addition > limit:
            remaining = total - index
            suffix = f"- ... e mais {remaining} linha(s)"
            if selected and len("\n".join(selected + [suffix])) <= limit:
                selected.append(suffix)
            break

        selected.append(line)
        current += addition

    return "\n".join(selected) if selected else fallback


def public_bank_status_text() -> str:
    return (
        "A Char Page publica da Artix so expõe `Inventory` e `Badges`. "
        "Nao existe endpoint publico oficial de `Bank` detectado pelo bot."
    )


def parse_level(value: str) -> int:
    match = re.search(r"\d+", value or "")
    return int(match.group()) if match else 0


def profile_art_path_for_user(user_id: int) -> Path:
    return PROFILE_ART_DIR / f"profile_art_v3_{user_id}.png"


def build_profile_art(
    screenshot_path: Path,
    output_path: Path,
    display_name: str,
) -> Path:
    try:
        from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont, ImageOps
    except ImportError as exc:
        raise AQWCharacterUnavailable(
            "A biblioteca Pillow nao esta instalada; nao foi possivel gerar a arte do perfil."
        ) from exc

    source = Image.open(screenshot_path).convert("RGBA")
    width, height = source.size

    def load_font(size: int):
        candidates = [
            "C:/Windows/Fonts/georgia.ttf",
            "C:/Windows/Fonts/pala.ttf",
            "C:/Windows/Fonts/times.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSerif-Regular.ttf",
        ]
        for candidate in candidates:
            try:
                return ImageFont.truetype(candidate, size)
            except Exception:
                continue
        return ImageFont.load_default()

    torso_box = (
        int(width * 0.20),
        int(height * 0.02),
        int(width * 0.80),
        int(height * 0.92),
    )
    torso = source.crop(torso_box)
    torso = ImageOps.contain(torso, (330, 395), Image.Resampling.LANCZOS)

    # Fundo transparente para o icone ficar recortado como um badge/escudo.
    canvas = Image.new("RGBA", (512, 512), (0, 0, 0, 0))
    draw = ImageDraw.Draw(canvas)

    outer_points = [(256, 26), (392, 72), (438, 176), (421, 392), (256, 494), (91, 392), (74, 176), (120, 72)]
    mid_points = [(256, 43), (376, 84), (414, 183), (399, 378), (256, 468), (113, 378), (98, 183), (136, 84)]
    inner_points = [(256, 60), (360, 96), (391, 188), (378, 364), (256, 444), (134, 364), (121, 188), (152, 96)]

    shadow = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow)
    shifted = [(x + 6, y + 10) for x, y in outer_points]
    shadow_draw.polygon(shifted, fill=(0, 0, 0, 72))
    shadow = shadow.filter(ImageFilter.GaussianBlur(10))
    canvas.alpha_composite(shadow)

    draw.polygon(outer_points, fill=(72, 55, 42, 255))
    draw.polygon(mid_points, fill=(113, 89, 67, 255))

    shield_fill = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    shield_draw = ImageDraw.Draw(shield_fill)
    for y in range(70, 450):
        ratio = (y - 70) / 380
        top = (230, 223, 207)
        bottom = (155, 145, 124)
        color = tuple(int(top[index] * (1 - ratio) + bottom[index] * ratio) for index in range(3))
        shield_draw.line([(110, y), (402, y)], fill=color + (255,), width=1)

    for offset in range(-220, 240, 28):
        shield_draw.line(
            [(256 + offset, 70), (110 + offset // 2, 454)],
            fill=(255, 255, 255, 20),
            width=3,
        )

    mask = Image.new("L", canvas.size, 0)
    ImageDraw.Draw(mask).polygon(inner_points, fill=255)
    mask = mask.filter(ImageFilter.GaussianBlur(1.2))
    shield_fill.putalpha(mask)
    canvas.alpha_composite(shield_fill)

    character_layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    character_position = (256 - torso.width // 2, 82)
    character_layer.alpha_composite(torso, character_position)

    focus_mask = Image.new("L", canvas.size, 0)
    focus_draw = ImageDraw.Draw(focus_mask)
    focus_draw.ellipse((120, 62, 392, 444), fill=255)
    focus_draw.rounded_rectangle((152, 46, 360, 454), radius=96, fill=255)
    focus_mask = focus_mask.filter(ImageFilter.GaussianBlur(18))
    combined_mask = ImageChops.multiply(mask, focus_mask)
    character_layer.putalpha(combined_mask)
    canvas.alpha_composite(character_layer)

    highlight = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    highlight_draw = ImageDraw.Draw(highlight)
    highlight_draw.polygon(inner_points, outline=(255, 248, 231, 110), width=3)
    highlight_draw.line([(153, 98), (360, 98)], fill=(255, 250, 236, 80), width=2)
    highlight_draw.arc((122, 102, 390, 320), start=198, end=342, fill=(255, 255, 255, 40), width=6)
    canvas.alpha_composite(highlight)

    banner = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    banner_draw = ImageDraw.Draw(banner)
    banner_y = 326
    banner_main = [(74, banner_y), (438, banner_y), (398, banner_y + 78), (114, banner_y + 78)]
    left_tail = [(34, banner_y + 16), (74, banner_y), (74, banner_y + 78), (24, banner_y + 62)]
    right_tail = [(438, banner_y), (478, banner_y + 16), (488, banner_y + 62), (438, banner_y + 78)]
    banner_draw.polygon(left_tail, fill=(210, 201, 180, 255), outline=(155, 142, 117, 255))
    banner_draw.polygon(right_tail, fill=(210, 201, 180, 255), outline=(155, 142, 117, 255))
    banner_draw.polygon(banner_main, fill=(223, 214, 191, 255), outline=(154, 141, 116, 255))
    banner_draw.line([(97, banner_y + 12), (411, banner_y + 12)], fill=(255, 248, 227, 95), width=2)
    banner_draw.line([(112, banner_y + 62), (397, banner_y + 62)], fill=(180, 166, 139, 100), width=2)
    canvas.alpha_composite(banner)

    title_font = load_font(28)
    subtitle_font = load_font(16)
    draw = ImageDraw.Draw(canvas)

    title = display_name[:24]
    while title_font.getlength(title) > 280 and len(title) > 4:
        title = title[:-1]

    title_bbox = draw.textbbox((0, 0), title, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    title_height = title_bbox[3] - title_bbox[1]
    title_x = (512 - title_width) // 2
    title_y = banner_y + 18 - title_bbox[1]
    draw.text(
        (title_x, title_y),
        title,
        fill=(86, 67, 49),
        font=title_font,
        stroke_width=1,
        stroke_fill=(243, 239, 223),
    )

    subtitle = "AQW"
    subtitle_bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
    subtitle_x = 256 - (subtitle_bbox[2] - subtitle_bbox[0]) // 2
    draw.text((subtitle_x, 432 - subtitle_bbox[1]), subtitle, fill=(98, 84, 67, 155), font=subtitle_font)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(output_path, "PNG", optimize=True)
    return output_path


def generate_profile_art_for_user(
    user_id: int,
    nickname: str,
    *,
    force_refresh: bool = True,
) -> tuple[AQWCharacterData, Path]:
    character = aqw_service.fetch_character(nickname, include_image=True, force_refresh=force_refresh)
    if not character.image_path or not character.image_path.exists():
        raise AQWCharacterUnavailable("Nao foi possivel capturar a imagem do personagem para gerar a arte do perfil.")

    output_path = profile_art_path_for_user(user_id)
    try:
        build_profile_art(character.image_path, output_path, character.display_name)
    finally:
        if character.image_path.exists():
            character.image_path.unlink(missing_ok=True)

    return replace(character, image_path=None), output_path


async def ensure_profile_art_for_link(
    link: AccountLink,
    *,
    force_refresh: bool = False,
) -> Path:
    output_path = profile_art_path_for_user(link.discord_user_id)
    if output_path.exists() and not force_refresh:
        return output_path

    _, generated_path = await asyncio.to_thread(
        generate_profile_art_for_user,
        link.discord_user_id,
        link.aqw_nickname,
        force_refresh=force_refresh,
    )
    return generated_path


def load_profile_art_file(user_id: int) -> Optional[discord.File]:
    art_path = profile_art_path_for_user(user_id)
    if not art_path.exists():
        return None

    return discord.File(art_path, filename=art_path.name)


class AccountLinkRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._lock = Lock()
        self._connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self.snapshot_min_interval = int(os.getenv("AQW_HISTORY_MIN_INTERVAL", "10800"))
        self._setup()

    def _setup(self) -> None:
        with self._lock:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS linked_accounts (
                    discord_user_id INTEGER PRIMARY KEY,
                    discord_name TEXT NOT NULL,
                    aqw_nickname TEXT NOT NULL,
                    aqw_nickname_normalized TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_linked_accounts_nickname
                ON linked_accounts(aqw_nickname_normalized)
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS profile_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discord_user_id INTEGER NOT NULL,
                    aqw_nickname TEXT NOT NULL,
                    level INTEGER NOT NULL,
                    inventory_total INTEGER NOT NULL,
                    badge_total INTEGER NOT NULL,
                    farm_total INTEGER NOT NULL,
                    ultra_total INTEGER NOT NULL,
                    class_total INTEGER NOT NULL,
                    top_class_name TEXT NOT NULL,
                    top_class_rank INTEGER NOT NULL,
                    recorded_at TEXT NOT NULL,
                    source TEXT NOT NULL
                )
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_profile_snapshots_user_time
                ON profile_snapshots(discord_user_id, recorded_at DESC)
                """
            )
            self._connection.commit()

    def upsert_link(self, user: discord.abc.User, aqw_nickname: str) -> AccountLink:
        normalized = normalize_lookup_token(aqw_nickname)
        now = utc_now_iso()
        discord_name = safe_user_display(user)

        with self._lock:
            existing_for_nick = self._connection.execute(
                """
                SELECT discord_user_id
                FROM linked_accounts
                WHERE aqw_nickname_normalized = ?
                """,
                (normalized,),
            ).fetchone()

            if existing_for_nick and existing_for_nick["discord_user_id"] != user.id:
                raise ValueError("Esse personagem ja esta vinculado a outra conta do Discord.")

            current = self._connection.execute(
                """
                SELECT created_at, aqw_nickname_normalized
                FROM linked_accounts
                WHERE discord_user_id = ?
                """,
                (user.id,),
            ).fetchone()

            created_at = current["created_at"] if current else now
            nickname_changed = bool(current and current["aqw_nickname_normalized"] != normalized)

            self._connection.execute(
                """
                INSERT INTO linked_accounts (
                    discord_user_id,
                    discord_name,
                    aqw_nickname,
                    aqw_nickname_normalized,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(discord_user_id) DO UPDATE SET
                    discord_name = excluded.discord_name,
                    aqw_nickname = excluded.aqw_nickname,
                    aqw_nickname_normalized = excluded.aqw_nickname_normalized,
                    updated_at = excluded.updated_at
                """,
                (user.id, discord_name, aqw_nickname, normalized, created_at, now),
            )
            if nickname_changed:
                self._connection.execute(
                    "DELETE FROM profile_snapshots WHERE discord_user_id = ?",
                    (user.id,),
                )
            self._connection.commit()

            row = self._connection.execute(
                """
                SELECT *
                FROM linked_accounts
                WHERE discord_user_id = ?
                """,
                (user.id,),
            ).fetchone()

        return self._row_to_link(row) if row else None

    def remove_link(self, user_id: int) -> bool:
        with self._lock:
            cursor = self._connection.execute(
                "DELETE FROM linked_accounts WHERE discord_user_id = ?",
                (user_id,),
            )
            self._connection.execute(
                "DELETE FROM profile_snapshots WHERE discord_user_id = ?",
                (user_id,),
            )
            self._connection.commit()
            return cursor.rowcount > 0

    def get_link(self, user_id: int) -> Optional[AccountLink]:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT *
                FROM linked_accounts
                WHERE discord_user_id = ?
                """,
                (user_id,),
            ).fetchone()
        return self._row_to_link(row) if row else None

    def list_links(self) -> list[AccountLink]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT *
                FROM linked_accounts
                ORDER BY aqw_nickname COLLATE NOCASE ASC
                """
            ).fetchall()
        return [self._row_to_link(row) for row in rows]

    def record_snapshot(self, link: AccountLink, character: AQWCharacterData, source: str) -> bool:
        stats = profile_stats(character)
        farms = len([farm for farm in detect_farms(character) if farm.completed])
        ultras = len([ultra for ultra in detect_ultras(character) if ultra.completed])
        classes = ranked_classes(character)
        top_class_name = classes[0][0] if classes else "Nenhuma"
        top_class_rank = classes[0][1] if classes else 0
        level_value = parse_level(character.level)
        now = utc_now_iso()

        with self._lock:
            latest = self._connection.execute(
                """
                SELECT *
                FROM profile_snapshots
                WHERE discord_user_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (link.discord_user_id,),
            ).fetchone()

            if latest:
                same_metrics = (
                    latest["aqw_nickname"] == character.display_name
                    and latest["level"] == level_value
                    and latest["inventory_total"] == stats["inventory_total"]
                    and latest["badge_total"] == stats["badge_total"]
                    and latest["farm_total"] == farms
                    and latest["ultra_total"] == ultras
                    and latest["class_total"] == stats["class_total"]
                    and latest["top_class_name"] == top_class_name
                    and latest["top_class_rank"] == top_class_rank
                )
                last_recorded = datetime.fromisoformat(latest["recorded_at"])
                elapsed = (datetime.now(timezone.utc) - last_recorded).total_seconds()
                if same_metrics and elapsed < self.snapshot_min_interval:
                    return False

            self._connection.execute(
                """
                INSERT INTO profile_snapshots (
                    discord_user_id,
                    aqw_nickname,
                    level,
                    inventory_total,
                    badge_total,
                    farm_total,
                    ultra_total,
                    class_total,
                    top_class_name,
                    top_class_rank,
                    recorded_at,
                    source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    link.discord_user_id,
                    character.display_name,
                    level_value,
                    stats["inventory_total"],
                    stats["badge_total"],
                    farms,
                    ultras,
                    stats["class_total"],
                    top_class_name,
                    top_class_rank,
                    now,
                    source,
                ),
            )
            self._connection.commit()
            return True

    def list_recent_snapshots(self, user_id: int, limit: int = 8) -> list[ProfileSnapshot]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT *
                FROM profile_snapshots
                WHERE discord_user_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        return [self._row_to_snapshot(row) for row in rows]

    @staticmethod
    def _row_to_link(row: sqlite3.Row) -> AccountLink:
        return AccountLink(
            discord_user_id=row["discord_user_id"],
            discord_name=row["discord_name"],
            aqw_nickname=row["aqw_nickname"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_snapshot(row: sqlite3.Row) -> ProfileSnapshot:
        return ProfileSnapshot(
            discord_user_id=row["discord_user_id"],
            aqw_nickname=row["aqw_nickname"],
            level=row["level"],
            inventory_total=row["inventory_total"],
            badge_total=row["badge_total"],
            farm_total=row["farm_total"],
            ultra_total=row["ultra_total"],
            class_total=row["class_total"],
            top_class_name=row["top_class_name"],
            top_class_rank=row["top_class_rank"],
            recorded_at=row["recorded_at"],
            source=row["source"],
        )


class AQWCharacterService:
    """
    Responsavel por:
    - Baixar o HTML publico da Char Page.
    - Extrair dados fixos da pagina com BeautifulSoup.
    - Ler o ccid interno para buscar badges e inventario.
    - Tirar screenshot do personagem via navegador headless.

    Estrutura validada na Char Page atual:
    - Nome: div.card-header h1
    - Titulo: div.card-header h4
    - Dados de perfil/equipados: labels dentro de div.card-body
    - Inventario: GET /CharPage/Inventory?ccid=<id>
    - Badges/conquistas: GET /CharPage/Badges?ccid=<id>
    - Imagem do personagem: renderizada no <ruffle-embed>
    """

    BASE_URL = "https://account.aq.com/CharPage"
    INVENTORY_URL = "https://account.aq.com/CharPage/Inventory"
    BADGES_URL = "https://account.aq.com/CharPage/Badges"
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
    INVENTORY_TYPE_ORDER = [
        "Class",
        "Armor",
        "Helm",
        "Cape",
        "Sword",
        "Axe",
        "Gauntlet",
        "Dagger",
        "HandGun",
        "Rifle",
        "Gun",
        "Whip",
        "Bow",
        "Mace",
        "Polearm",
        "Staff",
        "Wand",
        "Pet",
        "Item",
        "Quest Item",
        "Resource",
        "Necklace",
        "Misc",
        "Ground",
        "House",
        "Wall Item",
        "Floor Item",
    ]
    RESOURCE_TYPES = {"Item", "Quest Item", "Resource"}
    PROFILE_CACHE_TTL = int(os.getenv("AQW_PROFILE_CACHE_TTL", "300"))

    def __init__(self) -> None:
        self._headers = {
            "User-Agent": self.USER_AGENT,
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        self._cache: dict[str, tuple[float, AQWCharacterData]] = {}
        self._cache_lock = Lock()

    def invalidate_character_cache(self, nickname: str) -> None:
        cache_key = normalize_lookup_token(nickname)
        with self._cache_lock:
            self._cache.pop(cache_key, None)

    def fetch_character(
        self,
        nickname: str,
        include_image: bool = False,
        force_refresh: bool = False,
    ) -> AQWCharacterData:
        nickname = nickname.strip()
        if not nickname:
            raise AQWCharacterUnavailable("Voce precisa informar um nickname valido.")

        cache_key = normalize_lookup_token(nickname)
        cached_profile: Optional[AQWCharacterData] = None

        if not force_refresh:
            with self._cache_lock:
                cached = self._cache.get(cache_key)
            if cached and (time.time() - cached[0]) < self.PROFILE_CACHE_TTL:
                cached_profile = replace(cached[1], image_path=None)

        if cached_profile is None:
            session = self._create_session()
            try:
                cached_profile = self._fetch_character_core(session, nickname)
            finally:
                session.close()

            with self._cache_lock:
                self._cache[cache_key] = (time.time(), replace(cached_profile, image_path=None))

        if include_image:
            image_path = self._capture_character_image(cached_profile.url, cached_profile.display_name)
            return replace(cached_profile, image_path=image_path)

        return replace(cached_profile, image_path=None)

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(self._headers)
        return session

    def _fetch_character_core(self, session: requests.Session, nickname: str) -> AQWCharacterData:
        char_url = f"{self.BASE_URL}?id={quote_plus(nickname)}"
        response = session.get(char_url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        self._raise_if_charpage_invalid(soup, response.text)

        ccid = self._extract_ccid(response.text)
        display_name = self._safe_text(soup.select_one("div.card-header h1"))
        title = self._safe_text(soup.select_one("div.card-header h4"))

        if not display_name:
            raise AQWCharacterUnavailable(
                "A Char Page foi carregada, mas o nome do personagem nao foi encontrado."
            )

        labeled_values = self._extract_labeled_values(soup)
        equipped = {
            "Class": labeled_values.get("Class", "Nao informado"),
            "Weapon": labeled_values.get("Weapon", "Nao informado"),
            "Armor": labeled_values.get("Armor", "Nao informado"),
            "Helm": labeled_values.get("Helm", "Nao informado"),
            "Cape": labeled_values.get("Cape", "Nao informado"),
            "Pet": labeled_values.get("Pet", "Nao informado"),
            "Misc": labeled_values.get("Misc", "Nao informado"),
        }

        badges = self._fetch_badges(session, ccid, soup)
        inventory = self._fetch_inventory(session, ccid)

        return AQWCharacterData(
            ccid=ccid,
            queried_nickname=nickname,
            display_name=display_name,
            title=title or "Sem titulo publico",
            level=labeled_values.get("Level", "Desconhecido"),
            faction=labeled_values.get("Faction", "Desconhecida"),
            guild=labeled_values.get("Guild", "Sem guild"),
            url=char_url,
            equipped=equipped,
            inventory=inventory,
            badges=badges,
            image_path=None,
        )

    def _raise_if_charpage_invalid(self, soup: BeautifulSoup, raw_html: str) -> None:
        alert = soup.select_one("#serveralert")
        alert_text = self._safe_text(alert)
        if alert_text:
            lowered = alert_text.lower()
            if "not found" in lowered:
                raise AQWCharacterNotFound(
                    "Nao encontrei esse personagem na Char Page publica do AQW."
                )
            raise AQWCharacterUnavailable(
                f"A Char Page retornou um aviso do servidor: {alert_text}"
            )

        if "var ccid =" not in raw_html:
            raise AQWCharacterUnavailable(
                "Nao foi possivel localizar o identificador interno do personagem."
            )

    def _extract_ccid(self, raw_html: str) -> int:
        match = re.search(r"var\s+ccid\s*=\s*(\d+);", raw_html)
        if not match:
            raise AQWCharacterUnavailable(
                "A pagina nao retornou o ccid necessario para buscar o inventario."
            )
        return int(match.group(1))

    def _extract_labeled_values(self, soup: BeautifulSoup) -> dict[str, str]:
        values: dict[str, str] = {}

        for label in soup.select("div.card-body label"):
            key = label.get_text(" ", strip=True).replace(":", "").strip()
            if not key:
                continue

            parts: list[str] = []
            for sibling in label.next_siblings:
                if isinstance(sibling, Tag) and sibling.name == "br":
                    break

                if isinstance(sibling, NavigableString):
                    text = str(sibling).strip()
                elif isinstance(sibling, Tag):
                    text = sibling.get_text(" ", strip=True)
                else:
                    text = ""

                if text:
                    parts.append(text)

            values[key] = " ".join(parts).strip()

        return values

    def _fetch_inventory(self, session: requests.Session, ccid: int) -> list[dict]:
        response = session.get(self.INVENTORY_URL, params={"ccid": ccid}, timeout=30)
        response.raise_for_status()

        if response.text.strip().lower() == "error":
            raise AQWCharacterUnavailable(
                "A pagina do personagem foi encontrada, mas o inventario nao esta publico."
            )

        try:
            data = response.json()
        except ValueError as exc:
            raise AQWCharacterUnavailable(
                "O endpoint de inventario retornou um formato inesperado."
            ) from exc

        if not isinstance(data, list):
            raise AQWCharacterUnavailable(
                "O endpoint de inventario retornou uma estrutura invalida."
            )

        return sorted(
            data,
            key=lambda item: (
                item.get("sortOrder", 999),
                item.get("strType", ""),
                item.get("strName", "").lower(),
            ),
        )

    def _fetch_badges(
        self,
        session: requests.Session,
        ccid: int,
        soup: BeautifulSoup,
    ) -> list[AQWBadge]:
        fallback = self._extract_inline_badges(soup)

        try:
            response = session.get(self.BADGES_URL, params={"ccid": ccid}, timeout=30)
            response.raise_for_status()
        except requests.RequestException:
            logger.warning("Falha ao consultar badges do ccid %s; usando fallback do HTML.", ccid)
            return fallback

        if response.text.strip().lower() == "error":
            return fallback

        try:
            data = response.json()
        except ValueError:
            logger.warning("Endpoint de badges retornou JSON invalido; usando fallback do HTML.")
            return fallback

        if not isinstance(data, list):
            return fallback

        badges = [
            AQWBadge(
                category=str(entry.get("sCategory", "")).strip() or "Sem categoria",
                title=str(entry.get("sTitle", "")).strip() or "Badge sem nome",
                description=str(entry.get("sDesc", "")).strip(),
                file_name=str(entry.get("sFileName", "")).strip(),
                subcategory=str(entry.get("sSubCategory", "")).strip(),
            )
            for entry in data
        ]
        return badges or fallback

    def _extract_inline_badges(self, soup: BeautifulSoup) -> list[AQWBadge]:
        seen: set[str] = set()
        badges: list[AQWBadge] = []

        for anchor in soup.select("div.card-body a[title]"):
            title = anchor.get("title", "").strip()
            if not title or title in seen:
                continue
            seen.add(title)
            badges.append(
                AQWBadge(
                    category="Destaques",
                    title=title,
                    description="Badge destacado no HTML da Char Page.",
                    file_name="",
                    subcategory="",
                )
            )

        return badges

    def _capture_character_image(self, char_url: str, nickname: str) -> Optional[Path]:
        """
        A imagem do personagem e renderizada pelo Ruffle dentro de <ruffle-embed>.
        Por isso nao basta pegar um <img> no HTML: abrimos a pagina em um navegador
        headless, iniciamos a renderizacao e usamos Page.captureScreenshot para recortar
        a area do personagem.
        """
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.support.ui import WebDriverWait
        except ImportError:
            logger.warning("Selenium nao instalado; screenshot do personagem indisponivel.")
            return None

        driver = None
        image_path: Optional[Path] = None

        try:
            started_at = time.perf_counter()
            options = Options()
            options.page_load_strategy = "eager"
            options.add_argument("--headless=new")
            options.add_argument("--window-size=1400,1800")
            options.add_argument("--mute-audio")
            options.add_argument("--autoplay-policy=no-user-gesture-required")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--no-sandbox")
            options.add_argument("--log-level=3")
            options.add_argument("--use-gl=swiftshader")
            options.add_argument("--enable-webgl")
            options.add_argument("--ignore-gpu-blocklist")

            chrome_binary = os.getenv("AQW_CHROME_BINARY")
            if chrome_binary:
                options.binary_location = chrome_binary

            service = (
                Service(os.getenv("CHROMEDRIVER_PATH"))
                if os.getenv("CHROMEDRIVER_PATH")
                else Service()
            )

            driver = webdriver.Chrome(service=service, options=options)
            driver.get(char_url)

            wait = WebDriverWait(driver, 25)
            ruffle = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ruffle-embed")))
            time.sleep(2)

            try:
                driver.execute_script(
                    """
                    const host = document.querySelector("ruffle-embed");
                    const root = host && host.shadowRoot;
                    if (!root) return false;

                    const warning = root.querySelector("#hardware-acceleration-modal");
                    if (warning) warning.classList.add("hidden");

                    const unmute = root.querySelector("#unmute-overlay");
                    if (unmute) unmute.classList.add("hidden");

                    const play = root.querySelector("#play-button");
                    if (play) {
                        play.click();
                        return true;
                    }

                    return false;
                    """
                )
            except Exception:
                logger.debug("Nao foi possivel controlar o shadow DOM do Ruffle.")

            try:
                driver.execute_script("arguments[0].click();", ruffle)
            except Exception:
                logger.debug("Clique de fallback no Ruffle falhou.")

            safe_nickname = re.sub(r"[^a-zA-Z0-9_-]+", "_", nickname).strip("_") or "aqw"
            image_path = Path(tempfile.gettempdir()) / f"aqw_{safe_nickname}_{uuid.uuid4().hex[:8]}.png"

            try:
                valid_streak = 0
                best_bytes: Optional[bytes] = None
                deadline = time.perf_counter() + 45

                while time.perf_counter() < deadline:
                    clip_bytes = self._capture_ruffle_clip(driver)
                    if not clip_bytes:
                        valid_streak = 0
                        time.sleep(2)
                        continue

                    if self._image_looks_rendered(clip_bytes):
                        best_bytes = clip_bytes
                        valid_streak += 1
                        if valid_streak >= 2:
                            break
                    else:
                        valid_streak = 0

                    time.sleep(2)

                if not best_bytes:
                    raise RuntimeError("Nao foi possivel validar o screenshot do personagem a tempo.")

                image_path.write_bytes(best_bytes)
            except Exception:
                logger.debug("Falha no recorte do Ruffle; usando screenshot da pagina inteira.")
                driver.save_screenshot(str(image_path))

            if image_path.exists() and image_path.stat().st_size > 0:
                logger.info(
                    "Screenshot de '%s' concluido em %.2fs.",
                    nickname,
                    time.perf_counter() - started_at,
                )
                return image_path

            return None
        except Exception as exc:
            logger.warning("Nao foi possivel capturar a imagem do personagem: %s", exc)
            if image_path and image_path.exists():
                image_path.unlink(missing_ok=True)
            return None
        finally:
            if driver:
                driver.quit()

    def _capture_ruffle_clip(self, driver) -> Optional[bytes]:
        rect = driver.execute_script(
            """
            const el = document.querySelector("ruffle-embed");
            if (!el) return null;
            const r = el.getBoundingClientRect();
            if (r.width <= 0 || r.height <= 0) return null;
            return {
                x: r.left + window.scrollX,
                y: r.top + window.scrollY,
                width: r.width,
                height: r.height
            };
            """
        )

        if not rect:
            return None

        shot = driver.execute_cdp_cmd(
            "Page.captureScreenshot",
            {
                "format": "png",
                "clip": {
                    "x": rect["x"],
                    "y": rect["y"],
                    "width": rect["width"],
                    "height": rect["height"],
                    "scale": 1,
                },
            },
        )
        return base64.b64decode(shot["data"])

    def _image_looks_rendered(self, image_bytes: bytes) -> bool:
        try:
            from PIL import Image, ImageStat
        except ImportError:
            return bool(image_bytes)

        try:
            with Image.open(io.BytesIO(image_bytes)) as image:
                sample = image.convert("RGB").resize((64, 64), Image.Resampling.BILINEAR)
                stat = ImageStat.Stat(sample)
                avg_std = sum(stat.stddev) / len(stat.stddev)
                colors = sample.getcolors(maxcolors=64 * 64) or []
                unique_count = len(colors) if colors else 4096
                dominant_ratio = (max((count for count, _ in colors), default=0) / (64 * 64)) if colors else 0
                pixels = sample.load()
                non_flat = sum(
                    1
                    for y in range(sample.height)
                    for x in range(sample.width)
                    if sum(pixels[x, y]) / 3 < 252
                )

                return (
                    avg_std >= 15
                    and unique_count >= 25
                    and dominant_ratio <= 0.92
                    and non_flat >= 600
                )
        except Exception:
            return False

    @staticmethod
    def _safe_text(element: Optional[Tag]) -> str:
        if not element:
            return ""
        return html.unescape(element.get_text(" ", strip=True))


aqw_service = AQWCharacterService()
link_repository = AccountLinkRepository(DB_PATH)


def build_class_rank(points: int) -> int:
    ranks = [0]
    for index in range(1, 10):
        points_to_rank = ((index + 1) ** 3) * 100
        if index > 1:
            ranks.append(points_to_rank + ranks[index - 1])
        else:
            ranks.append(points_to_rank + 100)

    rank = 1
    for threshold in ranks[1:]:
        if points < threshold:
            return rank
        rank += 1
    return rank


def ranked_classes(character: AQWCharacterData) -> list[tuple[str, int, int]]:
    classes: list[tuple[str, int, int]] = []
    for item in character.inventory:
        if item.get("strType") != "Class":
            continue
        points = int(item.get("intCount", 0) or 0)
        classes.append((item.get("strName", "Classe desconhecida"), build_class_rank(points), points))

    return sorted(classes, key=lambda entry: (-entry[1], -entry[2], entry[0].lower()))


def profile_stats(character: AQWCharacterData) -> dict[str, int]:
    ac_items = sum(1 for item in character.inventory if item.get("bCoins"))
    member_items = sum(1 for item in character.inventory if item.get("bUpgrade"))
    class_count = sum(1 for item in character.inventory if item.get("strType") == "Class")
    unique_types = len({item.get("strType", "Outros") for item in character.inventory})

    return {
        "inventory_total": len(character.inventory),
        "badge_total": len(character.badges),
        "class_total": class_count,
        "ac_total": ac_items,
        "member_total": member_items,
        "type_total": unique_types,
    }


def class_leaderboard_metrics(character: AQWCharacterData) -> tuple[int, int, int, str]:
    classes = ranked_classes(character)
    best_rank = classes[0][1] if classes else 0
    total_points = sum(points for _, _, points in classes)
    class_total = len(classes)
    top_class_name = classes[0][0] if classes else "Nenhuma"
    return best_rank, class_total, total_points, top_class_name


def inventory_type_counts(character: AQWCharacterData) -> Counter[str]:
    return Counter(item.get("strType", "Outros") for item in character.inventory)


def badge_category_counts(character: AQWCharacterData) -> Counter[str]:
    return Counter(badge.category for badge in character.badges)


def format_snapshot_label(recorded_at: str) -> str:
    try:
        parsed = datetime.fromisoformat(recorded_at).astimezone()
        return parsed.strftime("%d/%m %H:%M")
    except ValueError:
        return recorded_at.replace("T", " ")[:16]


def build_search_pool(character: AQWCharacterData) -> list[tuple[str, str, str]]:
    pool: list[tuple[str, str, str]] = []

    for source, value in character.equipped.items():
        if value and value != "Nao informado":
            pool.append((f"Equipado ({source})", value, normalize_lookup_token(value)))

    for item in character.inventory:
        name = str(item.get("strName", "")).strip()
        if name:
            pool.append((f"Inventario ({item.get('strType', 'Item')})", name, normalize_lookup_token(name)))

    for badge in character.badges:
        if badge.title:
            pool.append((f"Badge ({badge.category})", badge.title, normalize_lookup_token(badge.title)))

    return pool


def detect_progress(character: AQWCharacterData, definitions: tuple[FarmDefinition, ...]) -> list[FarmStatus]:
    pool = build_search_pool(character)
    detected: list[FarmStatus] = []

    for definition in definitions:
        status = FarmStatus(definition=definition, completed=False)
        for source, original_name, normalized_name in pool:
            for term in definition.terms:
                normalized_term = normalize_lookup_token(term)
                if not normalized_term:
                    continue
                if normalized_term in normalized_name or normalized_name in normalized_term:
                    status.completed = True
                    status.source = source
                    status.matched_name = original_name
                    break
            if status.completed:
                break
        detected.append(status)

    return detected


def detect_farms(character: AQWCharacterData) -> list[FarmStatus]:
    return detect_progress(character, FARM_DEFINITIONS)


def detect_ultras(character: AQWCharacterData) -> list[FarmStatus]:
    return detect_progress(character, ULTRA_DEFINITIONS)


def search_profile_entries(character: AQWCharacterData, query: str) -> list[tuple[str, str]]:
    token = normalize_lookup_token(query)
    if not token:
        return []

    matches: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for source, original_name, normalized_name in build_search_pool(character):
        if token not in normalized_name:
            continue

        key = (source, original_name)
        if key in seen:
            continue

        seen.add(key)
        matches.append(key)

    return matches


def prioritize_missing_goals(character: AQWCharacterData) -> tuple[list[FarmStatus], list[FarmStatus]]:
    ultras = detect_ultras(character)
    farms = detect_farms(character)
    missing_ultras = [ultra for ultra in ultras if not ultra.completed]
    missing_ultras_names = {ultra.definition.name for ultra in missing_ultras}
    missing_farms = [
        farm for farm in farms
        if not farm.completed and farm.definition.name not in missing_ultras_names
    ]
    return missing_ultras, missing_farms


def format_inventory_item(item: dict) -> str:
    item_type = item.get("strType", "Item")
    name = item.get("strName", "Desconhecido")
    count = int(item.get("intCount", 1) or 1)

    if item_type == "Class":
        name = f"{name} (Rank {build_class_rank(count)})"
    elif item_type in AQWCharacterService.RESOURCE_TYPES and count > 1:
        name = f"{name} x{count}"

    tags: list[str] = []
    if item.get("bCoins"):
        tags.append("AC")
    if item.get("bUpgrade"):
        tags.append("Member")
    if tags:
        name = f"{name} [{' / '.join(tags)}]"

    return f"- {name}"


def build_inventory_fields(
    inventory: list[dict],
    max_fields: int = 8,
    max_items_per_field: int = 8,
) -> list[tuple[str, str]]:
    grouped: OrderedDict[str, list[dict]] = OrderedDict()

    for item_type in AQWCharacterService.INVENTORY_TYPE_ORDER:
        subset = [item for item in inventory if item.get("strType") == item_type]
        if subset:
            grouped[item_type] = subset

    for item in inventory:
        item_type = item.get("strType", "Outros")
        if item_type not in grouped:
            grouped[item_type] = [entry for entry in inventory if entry.get("strType") == item_type]

    fields: list[tuple[str, str]] = []
    overflow_types: list[str] = []

    for index, (item_type, items) in enumerate(grouped.items(), start=1):
        if index > max_fields:
            overflow_types.append(f"- {item_type} ({len(items)})")
            continue

        lines = [format_inventory_item(item) for item in items[:max_items_per_field]]
        remaining = len(items) - max_items_per_field
        if remaining > 0:
            lines.append(f"- ... e mais {remaining} item(ns)")

        fields.append((f"{item_type} ({len(items)})", build_field_value(lines)))

    if overflow_types:
        fields.append(("Outros tipos", build_field_value(overflow_types)))

    return fields


def build_character_embed(character: AQWCharacterData, requester: discord.abc.User) -> tuple[discord.Embed, Optional[discord.File]]:
    embed = discord.Embed(
        title=character.display_name,
        url=character.url,
        description=(
            f"**Titulo:** {character.title}\n"
            f"**Level:** {character.level}\n"
            f"**Faction:** {character.faction}\n"
            f"**Guild:** {character.guild or 'Sem guild'}\n"
            f"[Abrir Char Page]({character.url})"
        ),
        color=discord.Color.gold(),
    )

    equipped_lines = [
        f"- **Class**: {character.equipped.get('Class', 'Nao informado')}",
        f"- **Weapon**: {character.equipped.get('Weapon', 'Nao informado')}",
        f"- **Armor**: {character.equipped.get('Armor', 'Nao informado')}",
        f"- **Helm**: {character.equipped.get('Helm', 'Nao informado')}",
        f"- **Cape**: {character.equipped.get('Cape', 'Nao informado')}",
        f"- **Pet**: {character.equipped.get('Pet', 'Nao informado')}",
        f"- **Misc**: {character.equipped.get('Misc', 'Nao informado')}",
    ]
    embed.add_field(name="Equipamentos atuais", value=build_field_value(equipped_lines), inline=False)

    if character.badges:
        badge_lines = [f"- {badge.title}" for badge in character.badges[:8]]
        embed.add_field(name="Badges em destaque", value=build_field_value(badge_lines), inline=False)

    for field_name, field_value in build_inventory_fields(character.inventory):
        embed.add_field(name=field_name, value=field_value, inline=False)

    embed.set_footer(text=f"Solicitado por {safe_user_display(requester)}")

    file: Optional[discord.File] = None
    if character.image_path and character.image_path.exists():
        filename = character.image_path.name
        file = discord.File(character.image_path, filename=filename)
        embed.set_image(url=f"attachment://{filename}")

    return embed, file


def build_profile_embeds(
    character: AQWCharacterData,
    target_user: discord.abc.User,
    link: AccountLink,
    portrait_attachment_name: Optional[str] = None,
) -> dict[str, discord.Embed]:
    stats = profile_stats(character)
    farms = detect_farms(character)
    ultras = detect_ultras(character)
    completed_farms = [farm for farm in farms if farm.completed]
    pending_farms = [farm for farm in farms if not farm.completed]
    completed_ultras = [ultra for ultra in ultras if ultra.completed]
    pending_ultras, prioritized_farms = prioritize_missing_goals(character)
    classes = ranked_classes(character)
    badge_counts = badge_category_counts(character)
    type_counts = inventory_type_counts(character)

    summary = discord.Embed(
        title=f"Perfil AQW de {safe_user_display(target_user)}",
        url=character.url,
        description=(
            f"**Conta AQW vinculada:** {character.display_name}\n"
            f"**Titulo:** {character.title}\n"
            f"**Level:** {character.level}\n"
            f"**Guild:** {character.guild or 'Sem guild'}\n"
            f"**Faction:** {character.faction}\n"
            f"[Abrir Char Page]({character.url})"
        ),
        color=discord.Color.blurple(),
    )
    if portrait_attachment_name:
        summary.set_image(url=f"attachment://{portrait_attachment_name}")
    summary.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)
    summary.add_field(
        name="Resumo geral",
        value=build_field_value(
            [
                f"- Itens no inventario: {stats['inventory_total']}",
                f"- Tipos de itens detectados: {stats['type_total']}",
                f"- Classes no inventario: {stats['class_total']}",
                f"- Badges/conquistas: {stats['badge_total']}",
                f"- Itens AC: {stats['ac_total']}",
                f"- Itens Member: {stats['member_total']}",
                f"- Farms detectadas: {len(completed_farms)}/{len(farms)}",
                f"- Ultras detectadas: {len(completed_ultras)}/{len(ultras)}",
            ]
        ),
        inline=False,
    )
    summary.add_field(
        name="Classes em destaque",
        value=build_field_value([f"- {name} (Rank {rank})" for name, rank, _ in classes[:8]]),
        inline=False,
    )
    summary.add_field(
        name="Farms endgame detectadas",
        value=build_field_value(
            [f"- {farm.definition.name}" for farm in completed_farms[:8]],
            fallback="Nenhuma farm meta detectada ainda.",
        ),
        inline=False,
    )
    summary.add_field(
        name="Categorias de conquistas",
        value=build_field_value(
            [f"- {category}: {count}" for category, count in badge_counts.most_common(8)],
            fallback="Nenhuma badge publica detectada.",
        ),
        inline=False,
    )
    summary.add_field(
        name="Status do Bank",
        value=public_bank_status_text(),
        inline=False,
    )
    summary.set_footer(text=f"Vinculado em {link.created_at} | Atualizado em {link.updated_at}")

    farms_embed = discord.Embed(
        title=f"Farms e metas de {character.display_name}",
        url=character.url,
        description=(
            f"**Progresso detectado:** {len(completed_farms)}/{len(farms)} farms meta\n"
            f"**Observacao:** a deteccao e baseada em inventario, equipamentos e badges publicos."
        ),
        color=discord.Color.gold(),
    )
    if portrait_attachment_name:
        farms_embed.set_thumbnail(url=f"attachment://{portrait_attachment_name}")
    farms_embed.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)
    farms_embed.add_field(
        name="Concluidas",
        value=build_field_value(
            [
                f"- {farm.definition.name} ({farm.definition.category})"
                + (f" via {farm.source}: {farm.matched_name}" if farm.source and farm.matched_name else "")
                for farm in completed_farms
            ],
            fallback="Nenhuma das farms monitoradas foi encontrada.",
        ),
        inline=False,
    )
    farms_embed.add_field(
        name="Pendentes",
        value=build_field_value(
            [f"- {farm.definition.name} ({farm.definition.category})" for farm in pending_farms],
            fallback="Todas as farms monitoradas foram detectadas.",
        ),
        inline=False,
    )
    farms_embed.add_field(
        name="Sugestao de uso",
        value=build_field_value([f"- {farm.definition.name}: {farm.definition.note}" for farm in pending_farms[:6]]),
        inline=False,
    )

    achievements = discord.Embed(
        title=f"Conquistas de {character.display_name}",
        url=character.url,
        description=f"**Badges publicas detectadas:** {len(character.badges)}",
        color=discord.Color.green(),
    )
    if portrait_attachment_name:
        achievements.set_thumbnail(url=f"attachment://{portrait_attachment_name}")
    achievements.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)
    achievements.add_field(
        name="Categorias",
        value=build_field_value(
            [f"- {category}: {count}" for category, count in badge_counts.most_common(10)],
            fallback="Nenhuma badge publica detectada.",
        ),
        inline=False,
    )
    achievements.add_field(
        name="Titulos em destaque",
        value=build_field_value(
            [f"- [{badge.category}] {badge.title}" for badge in character.badges[:14]],
            fallback="Nenhuma conquista publica detectada.",
        ),
        inline=False,
    )

    goals_embed = discord.Embed(
        title=f"Metas de {character.display_name}",
        url=character.url,
        description=(
            f"**Discord:** {target_user.mention}\n"
            f"**Foco sugerido:** metas endgame que ainda nao apareceram no perfil publico."
        ),
        color=discord.Color.orange(),
    )
    if portrait_attachment_name:
        goals_embed.set_thumbnail(url=f"attachment://{portrait_attachment_name}")
    goals_embed.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)
    goals_embed.add_field(
        name="Prioridade de ultras",
        value=build_field_value(
            [f"- {ultra.definition.name}: {ultra.definition.note}" for ultra in pending_ultras[:8]],
            fallback="Nenhuma pendencia de ultra detectada entre as metas monitoradas.",
        ),
        inline=False,
    )
    goals_embed.add_field(
        name="Proximas farms recomendadas",
        value=build_field_value(
            [f"- {farm.definition.name}: {farm.definition.note}" for farm in prioritized_farms[:8]],
            fallback="As farms monitoradas principais ja foram detectadas.",
        ),
        inline=False,
    )
    goals_embed.add_field(name="Status do Bank", value=public_bank_status_text(), inline=False)

    classes_embed = discord.Embed(
        title=f"Classes de {character.display_name}",
        url=character.url,
        description=(
            f"**Discord:** {target_user.mention}\n"
            f"**Classe equipada:** {character.equipped.get('Class', 'Nao informado')}"
        ),
        color=discord.Color.dark_magenta(),
    )
    if portrait_attachment_name:
        classes_embed.set_thumbnail(url=f"attachment://{portrait_attachment_name}")
    classes_embed.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)
    classes_embed.add_field(
        name="Top classes por rank",
        value=build_field_value(
            [f"- {name} (Rank {rank} | Pontos {points})" for name, rank, points in classes[:15]],
            fallback="Nenhuma classe publica detectada no inventario.",
        ),
        inline=False,
    )
    classes_embed.add_field(
        name="Classes meta detectadas",
        value=build_field_value(
            [
                f"- {farm.definition.name}"
                for farm in farms
                if farm.completed and farm.definition.category == "Classes de farm"
            ],
            fallback="Nenhuma classe meta monitorada foi detectada.",
        ),
        inline=False,
    )
    classes_embed.add_field(name="Status do Bank", value=public_bank_status_text(), inline=False)

    ultras_embed = discord.Embed(
        title=f"Ultras de {character.display_name}",
        url=character.url,
        description=(
            f"**Progressao detectada:** {len(completed_ultras)}/{len(ultras)} metas de ultras\n"
            f"**Observacao:** deteccao baseada no inventario/equipados publicos."
        ),
        color=discord.Color.red(),
    )
    if portrait_attachment_name:
        ultras_embed.set_thumbnail(url=f"attachment://{portrait_attachment_name}")
    ultras_embed.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)
    ultras_embed.add_field(
        name="Concluidas",
        value=build_field_value(
            [
                f"- {ultra.definition.name} ({ultra.definition.category})"
                + (f" via {ultra.source}: {ultra.matched_name}" if ultra.source and ultra.matched_name else "")
                for ultra in completed_ultras
            ],
            fallback="Nenhuma das metas de ultra monitoradas foi encontrada.",
        ),
        inline=False,
    )
    ultras_embed.add_field(
        name="Pendentes",
        value=build_field_value(
            [f"- {ultra.definition.name} ({ultra.definition.category})" for ultra in ultras if not ultra.completed],
            fallback="Todas as metas de ultra monitoradas foram detectadas.",
        ),
        inline=False,
    )
    ultras_embed.add_field(
        name="Status do Bank",
        value=public_bank_status_text(),
        inline=False,
    )

    inventory = discord.Embed(
        title=f"Inventario resumido de {character.display_name}",
        url=character.url,
        description="Visao resumida das categorias mais fortes do perfil publico.",
        color=discord.Color.dark_teal(),
    )
    if portrait_attachment_name:
        inventory.set_thumbnail(url=f"attachment://{portrait_attachment_name}")
    inventory.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)
    inventory.add_field(
        name="Equipado agora",
        value=build_field_value(
            [
                f"- Class: {character.equipped.get('Class', 'Nao informado')}",
                f"- Weapon: {character.equipped.get('Weapon', 'Nao informado')}",
                f"- Armor: {character.equipped.get('Armor', 'Nao informado')}",
                f"- Helm: {character.equipped.get('Helm', 'Nao informado')}",
                f"- Cape: {character.equipped.get('Cape', 'Nao informado')}",
                f"- Pet: {character.equipped.get('Pet', 'Nao informado')}",
                f"- Misc: {character.equipped.get('Misc', 'Nao informado')}",
            ]
        ),
        inline=False,
    )
    inventory.add_field(
        name="Categorias do inventario",
        value=build_field_value(
            [f"- {item_type}: {count}" for item_type, count in type_counts.most_common(10)],
            fallback="Nenhuma categoria detectada.",
        ),
        inline=False,
    )
    inventory.add_field(
        name="Classes com maior rank",
        value=build_field_value([f"- {name} (Rank {rank})" for name, rank, _ in classes[:10]]),
        inline=False,
    )
    inventory.add_field(
        name="Status do Bank",
        value=public_bank_status_text(),
        inline=False,
    )

    return {
        "summary": summary,
        "farms": farms_embed,
        "achievements": achievements,
        "goals": goals_embed,
        "classes": classes_embed,
        "ultras": ultras_embed,
        "inventory": inventory,
    }


def build_achievements_embed(
    character: AQWCharacterData,
    target_user: discord.abc.User,
    category_filter: Optional[str] = None,
) -> discord.Embed:
    filtered = character.badges
    if category_filter:
        filtered = [
            badge for badge in character.badges
            if category_filter.lower() in badge.category.lower()
            or category_filter.lower() in badge.title.lower()
        ]

    badge_counts = Counter(badge.category for badge in filtered)

    embed = discord.Embed(
        title=f"Conquistas de {character.display_name}",
        url=character.url,
        description=(
            f"**Discord:** {target_user.mention}\n"
            f"**Filtro:** {category_filter or 'Nenhum'}\n"
            f"**Resultados:** {len(filtered)} badge(s)"
        ),
        color=discord.Color.green(),
    )
    embed.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)
    embed.add_field(
        name="Categorias",
        value=build_field_value(
            [f"- {category}: {count}" for category, count in badge_counts.most_common(10)],
            fallback="Nenhuma badge encontrada para esse filtro.",
        ),
        inline=False,
    )
    embed.add_field(
        name="Lista de conquistas",
        value=build_field_value(
            [f"- [{badge.category}] {badge.title}" for badge in filtered[:18]],
            fallback="Nenhuma badge encontrada para esse filtro.",
        ),
        inline=False,
    )
    return embed


def build_goals_embed(character: AQWCharacterData, target_user: discord.abc.User) -> discord.Embed:
    pending_ultras, pending_farms = prioritize_missing_goals(character)
    completed_farms = len([farm for farm in detect_farms(character) if farm.completed])
    completed_ultras = len([ultra for ultra in detect_ultras(character) if ultra.completed])

    embed = discord.Embed(
        title=f"Metas endgame de {character.display_name}",
        url=character.url,
        description=(
            f"**Discord:** {target_user.mention}\n"
            f"**Farms detectadas:** {completed_farms}/{len(FARM_DEFINITIONS)}\n"
            f"**Ultras detectadas:** {completed_ultras}/{len(ULTRA_DEFINITIONS)}"
        ),
        color=discord.Color.orange(),
    )
    embed.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)
    embed.add_field(
        name="Ultras para perseguir agora",
        value=build_field_value(
            [f"- {ultra.definition.name}: {ultra.definition.note}" for ultra in pending_ultras[:10]],
            fallback="Nenhuma pendencia de ultra detectada.",
        ),
        inline=False,
    )
    embed.add_field(
        name="Farms recomendadas",
        value=build_field_value(
            [f"- {farm.definition.name}: {farm.definition.note}" for farm in pending_farms[:10]],
            fallback="As farms monitoradas ja foram detectadas no perfil publico.",
        ),
        inline=False,
    )
    embed.add_field(name="Status do Bank", value=public_bank_status_text(), inline=False)
    return embed


def build_classes_embed(character: AQWCharacterData, target_user: discord.abc.User) -> discord.Embed:
    classes = ranked_classes(character)
    meta_classes = [
        farm.definition.name
        for farm in detect_farms(character)
        if farm.completed and farm.definition.category == "Classes de farm"
    ]

    embed = discord.Embed(
        title=f"Classes de {character.display_name}",
        url=character.url,
        description=(
            f"**Discord:** {target_user.mention}\n"
            f"**Classe equipada:** {character.equipped.get('Class', 'Nao informado')}\n"
            f"**Total de classes detectadas:** {len(classes)}"
        ),
        color=discord.Color.dark_magenta(),
    )
    embed.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)
    embed.add_field(
        name="Ranking de classes",
        value=build_field_value(
            [f"- {name} (Rank {rank} | Pontos {points})" for name, rank, points in classes[:18]],
            fallback="Nenhuma classe publica detectada.",
        ),
        inline=False,
    )
    embed.add_field(
        name="Classes meta detectadas",
        value=build_field_value(
            [f"- {name}" for name in meta_classes],
            fallback="Nenhuma classe meta monitorada foi detectada.",
        ),
        inline=False,
    )
    embed.add_field(name="Status do Bank", value=public_bank_status_text(), inline=False)
    return embed


def build_item_search_embed(
    character: AQWCharacterData,
    target_user: discord.abc.User,
    query: str,
) -> discord.Embed:
    matches = search_profile_entries(character, query)

    embed = discord.Embed(
        title=f"Busca no perfil de {character.display_name}",
        url=character.url,
        description=(
            f"**Discord:** {target_user.mention}\n"
            f"**Busca:** `{query}`\n"
            f"**Resultados:** {len(matches)}"
        ),
        color=discord.Color.teal(),
    )
    embed.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)
    embed.add_field(
        name="Ocorrencias",
        value=build_field_value(
            [f"- {source}: {name}" for source, name in matches[:20]],
            fallback="Nada correspondente foi encontrado no inventario, equipados ou badges publicos.",
        ),
        inline=False,
    )
    embed.add_field(name="Status do Bank", value=public_bank_status_text(), inline=False)
    return embed


def build_compare_embed(
    left_character: AQWCharacterData,
    right_character: AQWCharacterData,
    left_user: discord.abc.User,
    right_user: discord.abc.User,
) -> discord.Embed:
    left_stats = profile_stats(left_character)
    right_stats = profile_stats(right_character)
    left_farms = detect_farms(left_character)
    right_farms = detect_farms(right_character)
    left_completed = {farm.definition.name for farm in left_farms if farm.completed}
    right_completed = {farm.definition.name for farm in right_farms if farm.completed}
    left_classes = ranked_classes(left_character)
    right_classes = ranked_classes(right_character)

    embed = discord.Embed(
        title=f"Comparativo AQW: {safe_user_display(left_user)} vs {safe_user_display(right_user)}",
        color=discord.Color.purple(),
    )
    embed.add_field(
        name=f"{safe_user_display(left_user)} -> {left_character.display_name}",
        value=build_field_value(
            [
                f"- Level: {left_character.level}",
                f"- Badges: {left_stats['badge_total']}",
                f"- Itens: {left_stats['inventory_total']}",
                f"- Farms detectadas: {len(left_completed)}",
                f"- Melhor classe: {left_classes[0][0]} (Rank {left_classes[0][1]})" if left_classes else "- Melhor classe: Nenhuma detectada",
            ]
        ),
        inline=False,
    )
    embed.add_field(
        name=f"{safe_user_display(right_user)} -> {right_character.display_name}",
        value=build_field_value(
            [
                f"- Level: {right_character.level}",
                f"- Badges: {right_stats['badge_total']}",
                f"- Itens: {right_stats['inventory_total']}",
                f"- Farms detectadas: {len(right_completed)}",
                f"- Melhor classe: {right_classes[0][0]} (Rank {right_classes[0][1]})" if right_classes else "- Melhor classe: Nenhuma detectada",
            ]
        ),
        inline=False,
    )
    embed.add_field(
        name="Diferencas de farms",
        value=build_field_value(
            [f"- So {safe_user_display(left_user)}: {name}" for name in sorted(left_completed - right_completed)]
            + [f"- So {safe_user_display(right_user)}: {name}" for name in sorted(right_completed - left_completed)],
            fallback="Os dois perfis tem as mesmas farms monitoradas.",
        ),
        inline=False,
    )
    return embed


def build_ultras_embed(character: AQWCharacterData, target_user: discord.abc.User) -> discord.Embed:
    ultras = detect_ultras(character)
    completed = [ultra for ultra in ultras if ultra.completed]
    pending = [ultra for ultra in ultras if not ultra.completed]

    embed = discord.Embed(
        title=f"Ultras de {character.display_name}",
        url=character.url,
        description=(
            f"**Discord:** {target_user.mention}\n"
            f"**Conclusao detectada:** {len(completed)}/{len(ultras)}"
        ),
        color=discord.Color.red(),
    )
    embed.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)
    embed.add_field(
        name="Concluidas",
        value=build_field_value(
            [
                f"- {ultra.definition.name}"
                + (f" via {ultra.source}: {ultra.matched_name}" if ultra.source and ultra.matched_name else "")
                for ultra in completed
            ],
            fallback="Nenhuma das metas de ultra monitoradas foi detectada.",
        ),
        inline=False,
    )
    embed.add_field(
        name="Pendentes",
        value=build_field_value(
            [f"- {ultra.definition.name}" for ultra in pending],
            fallback="Nenhuma pendencia de ultra detectada.",
        ),
        inline=False,
    )
    embed.add_field(name="Status do Bank", value=public_bank_status_text(), inline=False)
    return embed


def build_history_embed(
    target_user: discord.abc.User,
    link: AccountLink,
    snapshots: list[ProfileSnapshot],
) -> discord.Embed:
    latest = snapshots[0] if snapshots else None
    oldest = snapshots[-1] if len(snapshots) > 1 else latest

    embed = discord.Embed(
        title=f"Historico AQW de {safe_user_display(target_user)}",
        description=(
            f"**Conta AQW:** {link.aqw_nickname}\n"
            f"**Snapshots registrados:** {len(snapshots)}"
        ),
        color=discord.Color.dark_gold(),
    )
    embed.set_author(name=safe_user_display(target_user), icon_url=target_user.display_avatar.url)

    if latest:
        delta_level = latest.level - (oldest.level if oldest else latest.level)
        delta_badges = latest.badge_total - (oldest.badge_total if oldest else latest.badge_total)
        delta_farms = latest.farm_total - (oldest.farm_total if oldest else latest.farm_total)
        delta_ultras = latest.ultra_total - (oldest.ultra_total if oldest else latest.ultra_total)

        embed.add_field(
            name="Resumo de progresso",
            value=build_field_value(
                [
                    f"- Ultimo registro: {format_snapshot_label(latest.recorded_at)} via {latest.source}",
                    f"- Level: {latest.level} ({delta_level:+d})",
                    f"- Badges publicas: {latest.badge_total} ({delta_badges:+d})",
                    f"- Farms detectadas: {latest.farm_total} ({delta_farms:+d})",
                    f"- Ultras detectadas: {latest.ultra_total} ({delta_ultras:+d})",
                    f"- Classes detectadas: {latest.class_total}",
                    f"- Melhor classe: {latest.top_class_name} (Rank {latest.top_class_rank})",
                ]
            ),
            inline=False,
        )
        embed.add_field(
            name="Timeline recente",
            value=build_field_value(
                [
                    f"- {format_snapshot_label(snapshot.recorded_at)} | Lv {snapshot.level} | Badges {snapshot.badge_total} | Farms {snapshot.farm_total} | Ultras {snapshot.ultra_total}"
                    for snapshot in snapshots
                ],
                fallback="Ainda nao ha snapshots suficientes para exibir a timeline.",
            ),
            inline=False,
        )
    else:
        embed.add_field(
            name="Sem historico ainda",
            value="O bot ainda nao registrou snapshots suficientes desse usuario.",
            inline=False,
        )

    embed.add_field(name="Status do Bank", value=public_bank_status_text(), inline=False)
    return embed


async def resolve_guild_linked_members(
    guild: discord.Guild,
) -> list[tuple[discord.Member, AccountLink]]:
    resolved: list[tuple[discord.Member, AccountLink]] = []

    for link in link_repository.list_links():
        member = guild.get_member(link.discord_user_id)
        if member is None:
            try:
                member = await guild.fetch_member(link.discord_user_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                continue

        resolved.append((member, link))

    return resolved


async def gather_linked_guild_profiles(
    guild: discord.Guild,
) -> list[tuple[discord.Member, AccountLink, AQWCharacterData]]:
    resolved_members = await resolve_guild_linked_members(guild)
    semaphore = asyncio.Semaphore(4)

    async def worker(member: discord.Member, link: AccountLink) -> Optional[tuple[discord.Member, AccountLink, AQWCharacterData]]:
        async with semaphore:
            try:
                character = await asyncio.to_thread(
                    aqw_service.fetch_character,
                    link.aqw_nickname,
                    False,
                    False,
                )
                await asyncio.to_thread(link_repository.record_snapshot, link, character, "guild-scan")
            except Exception:
                logger.exception("Falha ao montar perfil AQW vinculado de %s (%s).", member.display_name, member.id)
                return None

            return member, link, character

    tasks = [worker(member, link) for member, link in resolved_members]
    results = await asyncio.gather(*tasks)
    return [result for result in results if result is not None]


def build_rankingfarms_embed(
    guild: discord.Guild,
    entries: list[tuple[discord.Member, AccountLink, AQWCharacterData]],
) -> discord.Embed:
    ranking = []
    for member, _, character in entries:
        farm_count = len([farm for farm in detect_farms(character) if farm.completed])
        ultra_count = len([ultra for ultra in detect_ultras(character) if ultra.completed])
        ranking.append((member, character, farm_count, ultra_count))

    ranking.sort(
        key=lambda entry: (
            -entry[2],
            -entry[3],
            -parse_level(entry[1].level),
            entry[1].display_name.lower(),
        )
    )

    embed = discord.Embed(
        title=f"Ranking de farms - {guild.name}",
        description=f"Usuarios vinculados avaliados: {len(ranking)}",
        color=discord.Color.gold(),
    )
    lines = [
        f"- #{index} {member.display_name} -> {character.display_name} | Farms {farm_count}/{len(FARM_DEFINITIONS)} | Ultras {ultra_count}/{len(ULTRA_DEFINITIONS)} | Level {character.level}"
        for index, (member, character, farm_count, ultra_count) in enumerate(ranking[:15], start=1)
    ]
    embed.add_field(
        name="Top progresso",
        value=build_field_value(lines, fallback="Nenhum usuario vinculado encontrado nesta guild."),
        inline=False,
    )
    embed.add_field(name="Status do Bank", value=public_bank_status_text(), inline=False)
    return embed


def build_topclasses_embed(
    guild: discord.Guild,
    entries: list[tuple[discord.Member, AccountLink, AQWCharacterData]],
) -> discord.Embed:
    ranking = []
    for member, _, character in entries:
        best_rank, class_total, total_points, top_class_name = class_leaderboard_metrics(character)
        ranking.append((member, character, best_rank, class_total, total_points, top_class_name))

    ranking.sort(
        key=lambda entry: (
            -entry[2],
            -entry[3],
            -entry[4],
            entry[1].display_name.lower(),
        )
    )

    embed = discord.Embed(
        title=f"Top classes - {guild.name}",
        description=f"Usuarios vinculados avaliados: {len(ranking)}",
        color=discord.Color.dark_magenta(),
    )
    embed.add_field(
        name="Ranking de classes",
        value=build_field_value(
            [
                f"- #{index} {member.display_name} -> {character.display_name} | Melhor classe {top_class_name} (Rank {best_rank}) | Classes {class_total}"
                for index, (member, character, best_rank, class_total, _, top_class_name) in enumerate(ranking[:15], start=1)
            ],
            fallback="Nenhum usuario vinculado encontrado nesta guild.",
        ),
        inline=False,
    )
    embed.add_field(name="Status do Bank", value=public_bank_status_text(), inline=False)
    return embed


def build_badges_ranking_embed(
    guild: discord.Guild,
    entries: list[tuple[discord.Member, AccountLink, AQWCharacterData]],
) -> discord.Embed:
    ranking = sorted(
        entries,
        key=lambda entry: (
            -len(entry[2].badges),
            -len([farm for farm in detect_farms(entry[2]) if farm.completed]),
            entry[2].display_name.lower(),
        ),
    )

    embed = discord.Embed(
        title=f"Ranking de badges - {guild.name}",
        description=f"Usuarios vinculados avaliados: {len(ranking)}",
        color=discord.Color.green(),
    )
    embed.add_field(
        name="Top badges publicas",
        value=build_field_value(
            [
                f"- #{index} {member.display_name} -> {character.display_name} | Badges {len(character.badges)} | Categoria lider {badge_category_counts(character).most_common(1)[0][0] if character.badges else 'Nenhuma'}"
                for index, (member, _, character) in enumerate(ranking[:15], start=1)
            ],
            fallback="Nenhum usuario vinculado encontrado nesta guild.",
        ),
        inline=False,
    )
    embed.add_field(name="Status do Bank", value=public_bank_status_text(), inline=False)
    return embed


def build_guildaqw_embed(
    guild: discord.Guild,
    entries: list[tuple[discord.Member, AccountLink, AQWCharacterData]],
) -> discord.Embed:
    ordered = sorted(entries, key=lambda entry: entry[0].display_name.lower())

    embed = discord.Embed(
        title=f"Guild AQW - {guild.name}",
        description=f"Membros vinculados encontrados: {len(ordered)}",
        color=discord.Color.blue(),
    )
    lines = []
    for member, _, character in ordered[:20]:
        farm_count = len([farm for farm in detect_farms(character) if farm.completed])
        ultra_count = len([ultra for ultra in detect_ultras(character) if ultra.completed])
        lines.append(
            f"- {member.display_name} -> {character.display_name} | Lv {character.level} | Farms {farm_count} | Ultras {ultra_count} | Badges {len(character.badges)}"
        )

    embed.add_field(
        name="Resumo lado a lado",
        value=build_field_value(lines, fallback="Nenhum usuario vinculado encontrado nesta guild."),
        inline=False,
    )
    embed.add_field(name="Status do Bank", value=public_bank_status_text(), inline=False)
    return embed


async def resolve_linked_profile(
    target_user: discord.abc.User,
    *,
    force_refresh: bool = False,
) -> tuple[AccountLink, AQWCharacterData]:
    link = link_repository.get_link(target_user.id)
    if not link:
        raise AQWCharacterUnavailable(
            f"{safe_user_display(target_user)} ainda nao vinculou uma conta AQW. Use /vincular primeiro."
        )

    character = await asyncio.to_thread(
        aqw_service.fetch_character,
        link.aqw_nickname,
        False,
        force_refresh,
    )
    await asyncio.to_thread(link_repository.record_snapshot, link, character, "profile")
    return link, character


async def send_public_character_panel(interaction: discord.Interaction, nickname: str) -> None:
    nickname = nickname.strip()
    if not nickname:
        if interaction.response.is_done():
            await interaction.followup.send("Informe um nickname valido.", ephemeral=True)
        else:
            await interaction.response.send_message("Informe um nickname valido.", ephemeral=True)
        return

    if not interaction.response.is_done():
        await interaction.response.send_message(
            f"Gerando painel publico para **{nickname}**. Aguarde alguns segundos...",
            ephemeral=False,
        )
        status_message = await interaction.original_response()
    else:
        status_message = await interaction.followup.send(
            f"Gerando painel publico para **{nickname}**. Aguarde alguns segundos...",
            wait=True,
        )

    target_channel = interaction.channel
    character: Optional[AQWCharacterData] = None

    try:
        character = await asyncio.to_thread(aqw_service.fetch_character, nickname, True, False)
        embed, file = build_character_embed(character, interaction.user)

        if target_channel:
            if file:
                await target_channel.send(embed=embed, file=file)
            else:
                embed.add_field(
                    name="Imagem indisponivel",
                    value=(
                        "Os dados do personagem foram encontrados, mas a captura da imagem "
                        "nao conseguiu ser renderizada no navegador headless."
                    ),
                    inline=False,
                )
                await target_channel.send(embed=embed)
        else:
            if file:
                await status_message.edit(content=None, embed=embed, attachments=[file])
            else:
                embed.add_field(
                    name="Imagem indisponivel",
                    value=(
                        "Os dados do personagem foram encontrados, mas a captura da imagem "
                        "nao conseguiu ser renderizada no navegador headless."
                    ),
                    inline=False,
                )
                await status_message.edit(content=None, embed=embed)

        try:
            await status_message.delete()
        except Exception:
            await status_message.edit(content=f"Painel de **{nickname}** enviado acima.", embed=None)
    except AQWCharacterNotFound as exc:
        await status_message.edit(content=str(exc), embed=None)
    except AQWCharacterUnavailable as exc:
        await status_message.edit(content=str(exc), embed=None)
    except requests.RequestException:
        logger.exception("Falha de rede ao consultar a Char Page.")
        await status_message.edit(
            content="Nao consegui acessar a Char Page do AQW agora. Tente novamente em instantes.",
            embed=None,
        )
    except Exception:
        logger.exception("Erro inesperado ao processar o painel publico.")
        await status_message.edit(
            content="Ocorreu um erro inesperado ao montar o painel do personagem.",
            embed=None,
        )
    finally:
        if character and character.image_path and character.image_path.exists():
            character.image_path.unlink(missing_ok=True)


class AQWProfileView(discord.ui.View):
    def __init__(
        self,
        target_user: discord.abc.User,
        link: AccountLink,
        character: AQWCharacterData,
        portrait_attachment_name: Optional[str] = None,
        *,
        initial_section: str = "summary",
    ) -> None:
        super().__init__(timeout=600)
        self.target_user = target_user
        self.link = link
        self.character = character
        self.portrait_attachment_name = portrait_attachment_name
        self.embeds = build_profile_embeds(character, target_user, link, portrait_attachment_name)
        self.current_section = initial_section
        self.charpage_link_button = discord.ui.Button(
            label="Abrir Char Page",
            style=discord.ButtonStyle.link,
            url=character.url,
            row=1,
        )
        self.add_item(self.charpage_link_button)
        self._set_button_styles()

    def _set_button_styles(self) -> None:
        for child in self.children:
            if not isinstance(child, discord.ui.Button) or not child.custom_id:
                continue

            if child.custom_id.startswith("profile:section:"):
                section = child.custom_id.split(":")[-1]
                child.style = discord.ButtonStyle.primary if section == self.current_section else discord.ButtonStyle.secondary
            elif child.custom_id == "profile:history":
                child.style = discord.ButtonStyle.primary if self.current_section == "history" else discord.ButtonStyle.secondary

    async def _switch_section(self, interaction: discord.Interaction, section: str) -> None:
        self.current_section = section
        self._set_button_styles()
        await interaction.response.edit_message(embed=self.embeds[section], view=self)

    @discord.ui.button(label="Resumo", style=discord.ButtonStyle.primary, custom_id="profile:section:summary", row=0)
    async def summary_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._switch_section(interaction, "summary")

    @discord.ui.button(label="Farms", style=discord.ButtonStyle.secondary, custom_id="profile:section:farms", row=0)
    async def farms_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._switch_section(interaction, "farms")

    @discord.ui.button(label="Conquistas", style=discord.ButtonStyle.secondary, custom_id="profile:section:achievements", row=0)
    async def achievements_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._switch_section(interaction, "achievements")

    @discord.ui.button(label="Metas", style=discord.ButtonStyle.secondary, custom_id="profile:section:goals", row=1)
    async def goals_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._switch_section(interaction, "goals")

    @discord.ui.button(label="Classes", style=discord.ButtonStyle.secondary, custom_id="profile:section:classes", row=1)
    async def classes_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._switch_section(interaction, "classes")

    @discord.ui.button(label="Historico", style=discord.ButtonStyle.secondary, custom_id="profile:history", row=2)
    async def history_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        snapshots = link_repository.list_recent_snapshots(self.target_user.id, limit=8)
        embed = build_history_embed(self.target_user, self.link, snapshots)
        self.current_section = "history"
        self._set_button_styles()
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Ultras", style=discord.ButtonStyle.secondary, custom_id="profile:section:ultras", row=0)
    async def ultras_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._switch_section(interaction, "ultras")

    @discord.ui.button(label="Inventario", style=discord.ButtonStyle.secondary, custom_id="profile:section:inventory", row=0)
    async def inventory_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._switch_section(interaction, "inventory")

    @discord.ui.button(label="Atualizar", style=discord.ButtonStyle.success, custom_id="profile:refresh", row=1)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.defer(thinking=False)
        try:
            refreshed = await asyncio.to_thread(
                aqw_service.fetch_character,
                self.link.aqw_nickname,
                False,
                True,
            )
            await asyncio.to_thread(link_repository.record_snapshot, self.link, refreshed, "refresh")
            self.character = refreshed
            self.embeds = build_profile_embeds(
                refreshed,
                self.target_user,
                self.link,
                self.portrait_attachment_name,
            )
            self.charpage_link_button.url = refreshed.url
            self._set_button_styles()
            await interaction.message.edit(embed=self.embeds[self.current_section], view=self)
        except Exception:
            logger.exception("Falha ao atualizar o perfil AQW interativo.")
            await interaction.followup.send(
                "Nao consegui atualizar o perfil agora. Tente novamente em alguns segundos.",
                ephemeral=True,
            )

    @discord.ui.button(label="Painel com screenshot", style=discord.ButtonStyle.success, custom_id="profile:panel", row=1)
    async def panel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await send_public_character_panel(interaction, self.link.aqw_nickname)


class VersionSelectionView(discord.ui.View):
    def __init__(self, author_id: int, linked_nickname: Optional[str]) -> None:
        super().__init__(timeout=180)
        self.author_id = author_id
        self.linked_nickname = linked_nickname
        self.linked_button.disabled = linked_nickname is None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "Apenas quem executou o comando pode usar este painel.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="AQW Padrao (Classico)", style=discord.ButtonStyle.success, custom_id="aqw:classic", row=0)
    async def classic_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_modal(CharacterNicknameModal())

    @discord.ui.button(label="Usar conta vinculada", style=discord.ButtonStyle.primary, custom_id="aqw:linked", row=0)
    async def linked_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not self.linked_nickname:
            await interaction.response.send_message(
                "Voce ainda nao vinculou uma conta AQW. Use /vincular primeiro.",
                ephemeral=True,
            )
            return

        await send_public_character_panel(interaction, self.linked_nickname)

    @discord.ui.button(
        label="AQW Infinity (Em breve)",
        style=discord.ButtonStyle.secondary,
        custom_id="aqw:infinity",
        row=1,
    )
    async def infinity_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_message(
            "O AQW Infinity ainda nao foi lancado. Esta funcao estara disponivel no futuro!",
            ephemeral=True,
        )


class CharacterNicknameModal(discord.ui.Modal, title="Consultar Character Page"):
    nickname = discord.ui.TextInput(
        label="Nickname do personagem",
        placeholder="Ex.: caio nick pi",
        max_length=25,
        required=True,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await send_public_character_panel(interaction, str(self.nickname))


intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)
bot.synced = False


async def clear_remote_global_commands() -> int:
    application_id = bot.application_id or (bot.user.id if bot.user else None)
    if not application_id:
        return 0

    # Remove o catalogo global remoto para evitar duplicacao visual
    # quando optamos por trabalhar apenas com comandos por guild.
    data = await bot.http.request(
        Route(
            "PUT",
            "/applications/{application_id}/commands",
            application_id=application_id,
        ),
        json=[],
    )
    return len(data) if isinstance(data, list) else 0


@bot.event
async def on_ready() -> None:
    if not bot.synced:
        try:
            cleared = await clear_remote_global_commands()
            logger.info("Slash commands globais remotos limpos: %s", cleared)
        except Exception:
            logger.exception("Falha ao limpar comandos globais remotos.")

        for guild in bot.guilds:
            try:
                bot.tree.clear_commands(guild=guild)
                bot.tree.copy_global_to(guild=guild)
                guild_synced = await bot.tree.sync(guild=guild)
                logger.info(
                    "Slash commands sincronizados na guild %s (%s): %s",
                    guild.name,
                    guild.id,
                    len(guild_synced),
                )
            except Exception:
                logger.exception("Falha ao sincronizar comandos na guild %s (%s).", guild.name, guild.id)

        bot.synced = True
        logger.info("Bot online como %s.", bot.user)
    else:
        logger.info("Bot online como %s.", bot.user)


@bot.tree.command(name="ping", description="Mostra a latencia atual do bot.")
async def ping(interaction: discord.Interaction) -> None:
    latency_ms = round(bot.latency * 1000)
    await interaction.response.send_message(f"Pong! Latencia atual: `{latency_ms}ms`")


@bot.tree.command(name="help", description="Exibe o menu de ajuda do bot.")
async def help_command(interaction: discord.Interaction) -> None:
    embed = discord.Embed(
        title="Menu de ajuda",
        description="Bot utilitario com slash commands, vinculo de contas, perfis AQW e painel com screenshot.",
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="Utilitarios",
        value=build_field_value(
            [
                "- /ping: exibe a latencia atual do bot.",
                "- /help: mostra este menu de ajuda.",
                "- /painel: gera um painel publico com screenshot do personagem.",
            ]
        ),
        inline=False,
    )
    embed.add_field(
        name="Conta e perfil",
        value=build_field_value(
            [
                "- /vincular: vincula seu Discord a um personagem publico do AQW.",
                "- /desvincular: remove a vinculacao atual.",
                "- /perfil: abre o perfil AQW interativo do usuario.",
                "- /comparar: compara dois perfis AQW vinculados.",
            ]
        ),
        inline=False,
    )
    embed.add_field(
        name="Analise AQW",
        value=build_field_value(
            [
                "- /farms: mostra farms monitoradas do perfil.",
                "- /metas: recomenda proximas metas endgame.",
                "- /classes: detalha classes e ranks detectados.",
                "- /historico: exibe snapshots simples de progresso do usuario.",
                "- /ultras: mostra progresso em metas ligadas a ultras.",
                "- /conquistas: lista badges publicas.",
                "- /buscaritem: procura itens, badges e equipados no perfil publico.",
            ]
        ),
        inline=False,
    )
    embed.add_field(
        name="Servidor",
        value=build_field_value(
            [
                "- /rankingfarms: rankeia os vinculados do servidor por farms detectadas.",
                "- /topclasses: ranking de classes por servidor.",
                "- /rankingbadges: ranking de badges publicas por servidor.",
                "- /guildaqw: lista membros vinculados com resumo lado a lado.",
            ]
        ),
        inline=False,
    )
    embed.set_footer(text="discord.py 2.x | app_commands | discord.ui | SQLite")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="painel", description="Abre o painel interativo da Character Page do AQW.")
@app_commands.checks.cooldown(1, 20.0, key=lambda interaction: interaction.user.id)
async def painel(interaction: discord.Interaction) -> None:
    linked = link_repository.get_link(interaction.user.id)

    embed = discord.Embed(
        title="Painel AQW",
        description=(
            "Escolha como deseja consultar a Character Page.\n\n"
            "- **AQW Padrao (Classico)**: informa manualmente um nickname.\n"
            "- **Usar conta vinculada**: gera o painel direto da conta ligada ao seu Discord.\n"
            "- **AQW Infinity (Em breve)**: exibe aviso de funcao futura."
        ),
        color=discord.Color.gold(),
    )
    await interaction.response.send_message(
        embed=embed,
        view=VersionSelectionView(interaction.user.id, linked.aqw_nickname if linked else None),
        ephemeral=False,
    )


@bot.tree.command(name="vincular", description="Vincula sua conta do Discord a um personagem publico do AQW.")
@app_commands.describe(nickname="Nickname do personagem no AQW")
async def vincular(interaction: discord.Interaction, nickname: str) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        character, art_path = await asyncio.to_thread(
            generate_profile_art_for_user,
            interaction.user.id,
            nickname,
            force_refresh=True,
        )
        link = link_repository.upsert_link(interaction.user, character.display_name)
        await asyncio.to_thread(link_repository.record_snapshot, link, character, "link")

        embed = discord.Embed(
            title="Conta vinculada com sucesso",
            description=(
                f"**Discord:** {interaction.user.mention}\n"
                f"**AQW:** {link.aqw_nickname}\n"
                f"**Level:** {character.level}\n"
                f"[Abrir Char Page]({character.url})"
            ),
            color=discord.Color.green(),
        )
        if art_path.exists():
            embed.set_image(url=f"attachment://{art_path.name}")
        embed.add_field(
            name="Proximos comandos",
            value=build_field_value(
                [
                    "- /perfil para abrir o perfil interativo",
                    "- /farms para ver farms detectadas",
                    "- /conquistas para listar badges",
                    "- /painel para gerar screenshot publico",
                ]
            ),
            inline=False,
        )
        art_file = load_profile_art_file(interaction.user.id)
        if art_file:
            await interaction.edit_original_response(content=None, embed=embed, attachments=[art_file])
        else:
            await interaction.edit_original_response(content=None, embed=embed)
    except ValueError as exc:
        await interaction.edit_original_response(content=str(exc), embed=None)
    except AQWCharacterNotFound as exc:
        await interaction.edit_original_response(content=str(exc), embed=None)
    except AQWCharacterUnavailable as exc:
        await interaction.edit_original_response(content=str(exc), embed=None)
    except requests.RequestException:
        logger.exception("Falha de rede no /vincular.")
        await interaction.edit_original_response(
            content="Nao consegui validar esse personagem agora. Tente novamente em instantes.",
            embed=None,
        )
    except Exception:
        logger.exception("Erro inesperado no /vincular.")
        await interaction.edit_original_response(
            content="Ocorreu um erro inesperado ao vincular a conta.",
            embed=None,
        )


@bot.tree.command(name="desvincular", description="Remove a vinculacao atual da sua conta AQW.")
async def desvincular(interaction: discord.Interaction) -> None:
    existing = link_repository.get_link(interaction.user.id)
    removed = link_repository.remove_link(interaction.user.id)
    art_path = profile_art_path_for_user(interaction.user.id)
    if art_path.exists():
        art_path.unlink(missing_ok=True)
    if existing:
        aqw_service.invalidate_character_cache(existing.aqw_nickname)
    if removed:
        await interaction.response.send_message("Sua conta AQW foi desvinculada com sucesso.", ephemeral=True)
    else:
        await interaction.response.send_message("Voce nao possui nenhuma conta AQW vinculada.", ephemeral=True)


@bot.tree.command(name="perfil", description="Mostra o perfil AQW vinculado de um usuario do Discord.")
@app_commands.describe(usuario="Usuario do Discord vinculado ao AQW")
@app_commands.checks.cooldown(1, 8.0, key=lambda interaction: interaction.user.id)
async def perfil(interaction: discord.Interaction, usuario: Optional[discord.User] = None) -> None:
    target = usuario or interaction.user
    await interaction.response.defer(thinking=True)

    try:
        link, character = await resolve_linked_profile(target)
        await ensure_profile_art_for_link(link, force_refresh=False)
        portrait_file = load_profile_art_file(target.id)
        portrait_name = portrait_file.filename if portrait_file else None
        view = AQWProfileView(target, link, character, portrait_name, initial_section="summary")
        if portrait_file:
            await interaction.edit_original_response(embed=view.embeds["summary"], view=view, attachments=[portrait_file])
        else:
            await interaction.edit_original_response(embed=view.embeds["summary"], view=view)
    except AQWCharacterUnavailable as exc:
        await interaction.edit_original_response(content=str(exc), embed=None, view=None)
    except requests.RequestException:
        logger.exception("Falha de rede no /perfil.")
        await interaction.edit_original_response(
            content="Nao consegui buscar o perfil AQW agora. Tente novamente em instantes.",
            embed=None,
            view=None,
        )
    except Exception:
        logger.exception("Erro inesperado no /perfil.")
        await interaction.edit_original_response(
            content="Ocorreu um erro inesperado ao montar o perfil.",
            embed=None,
            view=None,
        )


@bot.tree.command(name="historico", description="Exibe um historico simples de progresso do perfil AQW vinculado.")
@app_commands.describe(usuario="Usuario do Discord vinculado ao AQW")
async def historico(interaction: discord.Interaction, usuario: Optional[discord.User] = None) -> None:
    target = usuario or interaction.user
    await interaction.response.defer(thinking=True)

    try:
        link, character = await resolve_linked_profile(target)
        # Garante snapshot recente antes de exibir o historico.
        await asyncio.to_thread(link_repository.record_snapshot, link, character, "history")
        snapshots = link_repository.list_recent_snapshots(target.id, limit=8)
        embed = build_history_embed(target, link, snapshots)
        await interaction.edit_original_response(embed=embed)
    except AQWCharacterUnavailable as exc:
        await interaction.edit_original_response(content=str(exc), embed=None)
    except Exception:
        logger.exception("Erro inesperado no /historico.")
        await interaction.edit_original_response(
            content="Nao consegui montar o historico agora.",
            embed=None,
        )


@bot.tree.command(name="farms", description="Mostra as farms monitoradas do perfil AQW vinculado.")
@app_commands.describe(usuario="Usuario do Discord vinculado ao AQW")
async def farms(interaction: discord.Interaction, usuario: Optional[discord.User] = None) -> None:
    target = usuario or interaction.user
    await interaction.response.defer(thinking=True)

    try:
        link, character = await resolve_linked_profile(target)
        view = AQWProfileView(target, link, character, initial_section="farms")
        await interaction.edit_original_response(embed=view.embeds["farms"], view=view)
    except AQWCharacterUnavailable as exc:
        await interaction.edit_original_response(content=str(exc), embed=None, view=None)
    except Exception:
        logger.exception("Erro inesperado no /farms.")
        await interaction.edit_original_response(
            content="Nao consegui montar a leitura de farms agora.",
            embed=None,
            view=None,
        )


@bot.tree.command(name="metas", description="Recomenda proximas metas endgame para o perfil AQW vinculado.")
@app_commands.describe(usuario="Usuario do Discord vinculado ao AQW")
async def metas(interaction: discord.Interaction, usuario: Optional[discord.User] = None) -> None:
    target = usuario or interaction.user
    await interaction.response.defer(thinking=True)

    try:
        _, character = await resolve_linked_profile(target)
        embed = build_goals_embed(character, target)
        await interaction.edit_original_response(embed=embed)
    except AQWCharacterUnavailable as exc:
        await interaction.edit_original_response(content=str(exc), embed=None)
    except Exception:
        logger.exception("Erro inesperado no /metas.")
        await interaction.edit_original_response(
            content="Nao consegui montar as metas agora.",
            embed=None,
        )


@bot.tree.command(name="classes", description="Detalha as classes e ranks do perfil AQW vinculado.")
@app_commands.describe(usuario="Usuario do Discord vinculado ao AQW")
async def classes(interaction: discord.Interaction, usuario: Optional[discord.User] = None) -> None:
    target = usuario or interaction.user
    await interaction.response.defer(thinking=True)

    try:
        _, character = await resolve_linked_profile(target)
        embed = build_classes_embed(character, target)
        await interaction.edit_original_response(embed=embed)
    except AQWCharacterUnavailable as exc:
        await interaction.edit_original_response(content=str(exc), embed=None)
    except Exception:
        logger.exception("Erro inesperado no /classes.")
        await interaction.edit_original_response(
            content="Nao consegui montar a lista de classes agora.",
            embed=None,
        )


@bot.tree.command(name="conquistas", description="Lista badges publicas do perfil AQW vinculado.")
@app_commands.describe(
    usuario="Usuario do Discord vinculado ao AQW",
    categoria="Filtro opcional por categoria ou parte do titulo da badge",
)
async def conquistas(
    interaction: discord.Interaction,
    usuario: Optional[discord.User] = None,
    categoria: Optional[str] = None,
) -> None:
    target = usuario or interaction.user
    await interaction.response.defer(thinking=True)

    try:
        _, character = await resolve_linked_profile(target)
        embed = build_achievements_embed(character, target, categoria)
        await interaction.edit_original_response(embed=embed)
    except AQWCharacterUnavailable as exc:
        await interaction.edit_original_response(content=str(exc), embed=None)
    except Exception:
        logger.exception("Erro inesperado no /conquistas.")
        await interaction.edit_original_response(
            content="Nao consegui listar as conquistas agora.",
            embed=None,
        )


@bot.tree.command(name="buscaritem", description="Busca itens, equipados e badges no perfil AQW vinculado.")
@app_commands.describe(
    termo="Trecho do nome do item, classe ou badge",
    usuario="Usuario do Discord vinculado ao AQW",
)
async def buscaritem(
    interaction: discord.Interaction,
    termo: str,
    usuario: Optional[discord.User] = None,
) -> None:
    target = usuario or interaction.user
    termo = termo.strip()
    if not termo:
        await interaction.response.send_message("Informe um termo de busca valido.", ephemeral=True)
        return

    await interaction.response.defer(thinking=True)

    try:
        _, character = await resolve_linked_profile(target)
        embed = build_item_search_embed(character, target, termo)
        await interaction.edit_original_response(embed=embed)
    except AQWCharacterUnavailable as exc:
        await interaction.edit_original_response(content=str(exc), embed=None)
    except Exception:
        logger.exception("Erro inesperado no /buscaritem.")
        await interaction.edit_original_response(
            content="Nao consegui pesquisar esse termo agora.",
            embed=None,
        )


@bot.tree.command(name="ultras", description="Monitora progresso em itens/classes ligados a ultras.")
@app_commands.describe(usuario="Usuario do Discord vinculado ao AQW")
async def ultras(interaction: discord.Interaction, usuario: Optional[discord.User] = None) -> None:
    target = usuario or interaction.user
    await interaction.response.defer(thinking=True)

    try:
        _, character = await resolve_linked_profile(target)
        embed = build_ultras_embed(character, target)
        await interaction.edit_original_response(embed=embed)
    except AQWCharacterUnavailable as exc:
        await interaction.edit_original_response(content=str(exc), embed=None)
    except Exception:
        logger.exception("Erro inesperado no /ultras.")
        await interaction.edit_original_response(
            content="Nao consegui montar a leitura de ultras agora.",
            embed=None,
        )


@bot.tree.command(name="rankingfarms", description="Rankeia os usuarios vinculados do servidor pelas farms detectadas.")
async def rankingfarms(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Esse comando so pode ser usado dentro de um servidor.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True)

    try:
        entries = await gather_linked_guild_profiles(interaction.guild)
        embed = build_rankingfarms_embed(interaction.guild, entries)
        await interaction.edit_original_response(embed=embed)
    except Exception:
        logger.exception("Erro inesperado no /rankingfarms.")
        await interaction.edit_original_response(
            content="Nao consegui montar o ranking de farms agora.",
            embed=None,
        )


@bot.tree.command(name="topclasses", description="Rankeia os usuarios vinculados do servidor pelas classes detectadas.")
async def topclasses(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Esse comando so pode ser usado dentro de um servidor.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True)

    try:
        entries = await gather_linked_guild_profiles(interaction.guild)
        embed = build_topclasses_embed(interaction.guild, entries)
        await interaction.edit_original_response(embed=embed)
    except Exception:
        logger.exception("Erro inesperado no /topclasses.")
        await interaction.edit_original_response(
            content="Nao consegui montar o ranking de classes agora.",
            embed=None,
        )


@bot.tree.command(name="rankingbadges", description="Rankeia os usuarios vinculados do servidor pelas badges publicas.")
async def rankingbadges(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Esse comando so pode ser usado dentro de um servidor.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True)

    try:
        entries = await gather_linked_guild_profiles(interaction.guild)
        embed = build_badges_ranking_embed(interaction.guild, entries)
        await interaction.edit_original_response(embed=embed)
    except Exception:
        logger.exception("Erro inesperado no /rankingbadges.")
        await interaction.edit_original_response(
            content="Nao consegui montar o ranking de badges agora.",
            embed=None,
        )


@bot.tree.command(name="guildaqw", description="Lista os membros vinculados do servidor com resumo lado a lado.")
async def guildaqw(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Esse comando so pode ser usado dentro de um servidor.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True)

    try:
        entries = await gather_linked_guild_profiles(interaction.guild)
        embed = build_guildaqw_embed(interaction.guild, entries)
        await interaction.edit_original_response(embed=embed)
    except Exception:
        logger.exception("Erro inesperado no /guildaqw.")
        await interaction.edit_original_response(
            content="Nao consegui montar a lista da guild AQW agora.",
            embed=None,
        )


@bot.tree.command(name="comparar", description="Compara os perfis AQW vinculados de dois usuarios do Discord.")
@app_commands.describe(
    usuario_1="Primeiro usuario vinculado",
    usuario_2="Segundo usuario vinculado",
)
async def comparar(
    interaction: discord.Interaction,
    usuario_1: discord.User,
    usuario_2: discord.User,
) -> None:
    if usuario_1.id == usuario_2.id:
        await interaction.response.send_message(
            "Escolha dois usuarios diferentes para comparar.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True)

    try:
        _, left_character = await resolve_linked_profile(usuario_1)
        _, right_character = await resolve_linked_profile(usuario_2)
        embed = build_compare_embed(left_character, right_character, usuario_1, usuario_2)
        await interaction.edit_original_response(embed=embed)
    except AQWCharacterUnavailable as exc:
        await interaction.edit_original_response(content=str(exc), embed=None)
    except Exception:
        logger.exception("Erro inesperado no /comparar.")
        await interaction.edit_original_response(
            content="Nao consegui montar o comparativo agora.",
            embed=None,
        )


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: app_commands.AppCommandError,
) -> None:
    logger.exception("Erro em slash command: %s", error)

    message = "Ocorreu um erro ao executar o comando."
    if isinstance(error, app_commands.CommandOnCooldown):
        message = "Este comando esta em cooldown. Aguarde um pouco e tente novamente."

    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


if __name__ == "__main__":
    # Defina o token do bot via variavel de ambiente DISCORD_TOKEN.
    # Isso evita versionar segredo dentro do repositório.
    TOKEN = os.getenv("DISCORD_TOKEN")
    if not TOKEN:
        raise RuntimeError("Defina a variavel de ambiente DISCORD_TOKEN antes de iniciar o bot.")

    bot.run(TOKEN)
