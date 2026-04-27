#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base, relationship

from backend import config

metadata = MetaData()
Base = declarative_base(metadata=metadata)

engine = create_async_engine(config.sqlalchemy_database_async_uri, echo=config.enable_sqlalchemy_debugging)
Session: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)

# Use of 'db' in this project is now deprecated and will be removed in a future release. Use Session instead.
db = SQLAlchemy()


class Epg(Base):
    __tablename__ = "epgs"
    __table_args__ = (Index("ix_epgs_name", "name"),)
    id = Column(Integer, primary_key=True)

    enabled = Column(Boolean, nullable=False)
    name = Column(String(500))
    url = Column(Text)
    user_agent = Column(String(255), nullable=True)
    update_schedule = Column(String(16), nullable=False, default="12h")

    # Backref to all associated linked channels
    epg_channels = relationship("EpgChannels", back_populates="guide", cascade="all, delete-orphan")
    channels = relationship("Channel", back_populates="guide", cascade="all, delete-orphan")

    def __repr__(self):
        return "<Epg {}>".format(self.id)


class EpgChannels(Base):
    __tablename__ = "epg_channels"
    __table_args__ = (
        Index("ix_epg_channels_channel_id", "channel_id"),
        Index("ix_epg_channels_name", "name"),
        Index("ix_epg_channels_epg_id", "epg_id"),
        Index("ix_epg_channels_epg_id_channel_id", "epg_id", "channel_id"),
    )
    id = Column(Integer, primary_key=True)

    channel_id = Column(String(256))
    name = Column(String(500))
    icon_url = Column(Text)

    # Link with an epg
    epg_id = Column(Integer, ForeignKey("epgs.id"), nullable=False)

    guide = relationship("Epg", back_populates="epg_channels")

    # Backref to all associated linked channels
    epg_channel_programmes = relationship(
        "EpgChannelProgrammes", back_populates="channel", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return "<EpgChannels {}>".format(self.id)


class EpgChannelProgrammes(Base):
    """
    <programme start="20230423183001 +0100"0 stop="20230423190001 +100" start_timestamp="1682271001" stop_timestamp="1682272801" channel="some_channel_id" >
        <title>Programme Title</title>
        <desc>Programme description.</desc>
    </programme>
    """

    __tablename__ = "epg_channel_programmes"
    __table_args__ = (
        Index("ix_epg_channel_programmes_channel_id", "channel_id"),
        Index("ix_epg_channel_programmes_title", "title"),
        Index("ix_epg_channel_programmes_categories", "categories"),
        Index("ix_epg_channel_programmes_metadata_lookup_hash", "metadata_lookup_hash"),
        Index("ix_epg_channel_programmes_epg_channel_id", "epg_channel_id"),
        Index("ix_epg_channel_programmes_epg_channel_id_start", "epg_channel_id", "start"),
    )
    id = Column(Integer, primary_key=True)

    channel_id = Column(Text)
    title = Column(Text)
    sub_title = Column(Text)
    desc = Column(Text)
    series_desc = Column(Text)
    country = Column(Text)
    icon_url = Column(Text)
    start = Column(Text)
    stop = Column(Text)
    start_timestamp = Column(Text)
    stop_timestamp = Column(Text)
    categories = Column(Text)
    # Extended optional XMLTV / TVHeadend supported metadata (all nullable / optional)
    summary = Column(Text)
    keywords = Column(Text)  # JSON encoded list of keyword strings
    credits_json = Column(Text)  # JSON: {"actor":[],"director":[],...}
    video_colour = Column(Text)
    video_aspect = Column(Text)
    video_quality = Column(Text)
    subtitles_type = Column(Text)
    audio_described = Column(Boolean, nullable=True)  # True -> <audio-described />
    previously_shown_date = Column(Text)  # YYYY-MM-DD
    premiere = Column(Boolean, nullable=True)
    is_new = Column(Boolean, nullable=True)
    epnum_onscreen = Column(Text)
    epnum_xmltv_ns = Column(Text)
    epnum_dd_progid = Column(Text)
    star_rating = Column(Text)  # e.g. "3/5"
    production_year = Column(Text)  # <date>
    rating_system = Column(Text)
    rating_value = Column(Text)
    metadata_lookup_hash = Column(String(64))

    # Link with an epg channel
    epg_channel_id = Column(Integer, ForeignKey("epg_channels.id"), nullable=False)

    channel = relationship("EpgChannels", back_populates="epg_channel_programmes")

    def __repr__(self):
        return "<EpgChannelProgrammes {}>".format(self.id)


class EpgProgrammeMetadataCache(Base):
    __tablename__ = "epg_programme_metadata_cache"
    __table_args__ = (
        Index("ix_epg_programme_metadata_cache_expires_at", "expires_at"),
        Index("ix_epg_programme_metadata_cache_lookup_hash", "lookup_hash"),
        Index("ix_epg_programme_metadata_cache_match_status", "match_status"),
        UniqueConstraint("lookup_hash", name="epg_programme_metadata_cache_lookup_hash_key"),
    )

    id = Column(Integer, primary_key=True)
    lookup_hash = Column(String(64), nullable=False)
    lookup_title = Column(Text, nullable=True)
    lookup_sub_title = Column(Text, nullable=True)
    lookup_kind = Column(String(16), nullable=False, default="unknown")
    match_status = Column(String(16), nullable=False, default="no_match")
    provider = Column(String(32), nullable=False, default="tmdb")
    provider_item_type = Column(String(16), nullable=True)
    provider_item_id = Column(Integer, nullable=True)
    provider_series_id = Column(Integer, nullable=True)
    provider_season_number = Column(Integer, nullable=True)
    provider_episode_number = Column(Integer, nullable=True)
    cached_sub_title = Column(Text, nullable=True)
    cached_desc = Column(Text, nullable=True)
    cached_series_desc = Column(Text, nullable=True)
    cached_icon_url = Column(Text, nullable=True)
    cached_epnum_onscreen = Column(Text, nullable=True)
    cached_epnum_xmltv_ns = Column(Text, nullable=True)
    last_checked_at = Column(DateTime, nullable=False, default=func.now())
    expires_at = Column(DateTime, nullable=False)
    failure_count = Column(Integer, nullable=False, default=0)
    source_confidence = Column(Float, nullable=True)
    raw_result_json = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())

    def __repr__(self):
        return "<EpgProgrammeMetadataCache {}>".format(self.id)


class Playlist(Base):
    __tablename__ = "playlists"
    __table_args__ = (
        Index("ix_playlists_name", "name"),
        Index("ix_playlists_tvh_uuid", "tvh_uuid", unique=True),
    )
    id = Column(Integer, primary_key=True)

    enabled = Column(Boolean, nullable=False)
    connections = Column(Integer, nullable=False)
    name = Column(String(500))
    tvh_uuid = Column(String(64))
    url = Column(Text)
    account_type = Column(String(16), nullable=False, default="M3U")
    xc_username = Column(String(255), nullable=True)
    xc_password = Column(String(255), nullable=True)
    use_hls_proxy = Column(Boolean, nullable=False)
    use_custom_hls_proxy = Column(Boolean, nullable=False)
    hls_proxy_path = Column(String(256))
    chain_custom_hls_proxy = Column(Boolean, nullable=False, default=False)
    hls_proxy_use_ffmpeg = Column(Boolean, nullable=False, default=False)
    hls_proxy_prebuffer = Column(String(32), nullable=True, default="1M")
    hls_proxy_headers = Column(Text, nullable=True)
    user_agent = Column(String(255), nullable=True)
    xc_live_stream_format = Column(String(8), nullable=False, default="ts")
    update_schedule = Column(String(16), nullable=False, default="off")

    # Backref to all associated linked sources
    channel_sources = relationship("ChannelSource", back_populates="playlist", cascade="all, delete-orphan")
    playlist_streams = relationship("PlaylistStreams", back_populates="playlist", cascade="all, delete-orphan")
    xc_accounts = relationship("XcAccount", back_populates="playlist", cascade="all, delete-orphan")
    suggestions = relationship("ChannelSuggestion", back_populates="playlist", cascade="all, delete-orphan")

    def __repr__(self):
        return "<Playlist {}>".format(self.id)


class PlaylistStreams(Base):
    __tablename__ = "playlist_streams"
    __table_args__ = (
        Index("ix_playlist_streams_name", "name"),
        Index("ix_playlist_streams_channel_id", "channel_id"),
        Index("ix_playlist_streams_group_title", "group_title"),
        Index("ix_playlist_streams_tvg_id", "tvg_id"),
        Index("ix_playlist_streams_source_type", "source_type"),
        Index("ix_playlist_streams_xc_stream_id", "xc_stream_id"),
        Index("ix_playlist_streams_xc_category_id", "xc_category_id"),
        Index("ix_playlist_streams_xc_epg_channel_id", "xc_epg_channel_id"),
        Index("ix_playlist_streams_playlist_id", "playlist_id"),
        Index("ix_playlist_streams_playlist_id_url_hash", "playlist_id", "url_hash"),
    )
    id = Column(Integer, primary_key=True)

    name = Column(String(500))
    url = Column(Text)
    url_hash = Column(String(32))
    channel_id = Column(Text)
    group_title = Column(String(500))
    tvg_chno = Column(Integer)
    tvg_id = Column(String(500))
    tvg_logo = Column(Text)
    source_type = Column(String(16), default="M3U")
    xc_stream_id = Column(Integer)
    xc_category_id = Column(Integer)
    xc_epg_channel_id = Column(String(500))
    xc_tv_archive = Column(Boolean, nullable=False, default=False)
    xc_tv_archive_duration = Column(Integer, nullable=True)

    # Link with a playlist
    playlist_id = Column(Integer, ForeignKey("playlists.id"), nullable=True)

    playlist = relationship("Playlist", back_populates="playlist_streams")

    def __repr__(self):
        return "<PlaylistStreams {}>".format(self.id)


class XcAccount(Base):
    __tablename__ = "xc_accounts"
    __table_args__ = (Index("ix_xc_accounts_tvh_uuid", "tvh_uuid", unique=True),)
    id = Column(Integer, primary_key=True)

    playlist_id = Column(Integer, ForeignKey("playlists.id"), nullable=False)
    username = Column(String(255), nullable=False)
    password = Column(String(255), nullable=False)
    enabled = Column(Boolean, nullable=False, default=True)
    connection_limit = Column(Integer, nullable=False, default=1)
    label = Column(String(255), nullable=True)
    tvh_uuid = Column(String(64), nullable=True)

    playlist = relationship("Playlist", back_populates="xc_accounts")

    def __repr__(self):
        return "<XcAccount {}>".format(self.id)


class XcVodCategory(Base):
    __tablename__ = "xc_vod_categories"
    __table_args__ = (
        Index("ix_xc_vod_categories_playlist_id", "playlist_id"),
        Index("ix_xc_vod_categories_category_type", "category_type"),
        Index("ix_xc_vod_categories_upstream_category_id", "upstream_category_id"),
        Index("ix_xc_vod_categories_name", "name"),
        UniqueConstraint("playlist_id", "category_type", "upstream_category_id", name="uq_xc_vod_category_upstream"),
    )
    id = Column(Integer, primary_key=True)

    playlist_id = Column(Integer, ForeignKey("playlists.id", ondelete="CASCADE"), nullable=False)
    category_type = Column(String(16), nullable=False)
    upstream_category_id = Column(String(64), nullable=False)
    name = Column(String(500), nullable=False)
    parent_id = Column(String(64), nullable=True)

    playlist = relationship("Playlist")

    def __repr__(self):
        return "<XcVodCategory {}>".format(self.id)


class XcVodItem(Base):
    __tablename__ = "xc_vod_items"
    __table_args__ = (
        Index("ix_xc_vod_items_playlist_id", "playlist_id"),
        Index("ix_xc_vod_items_category_id", "category_id"),
        Index("ix_xc_vod_items_item_type", "item_type"),
        Index("ix_xc_vod_items_upstream_item_id", "upstream_item_id"),
        Index("ix_xc_vod_items_title", "title"),
        Index("ix_xc_vod_items_sort_title", "sort_title"),
        UniqueConstraint("playlist_id", "item_type", "upstream_item_id", name="uq_xc_vod_item_upstream"),
    )
    id = Column(Integer, primary_key=True)

    playlist_id = Column(Integer, ForeignKey("playlists.id", ondelete="CASCADE"), nullable=False)
    category_id = Column(Integer, ForeignKey("xc_vod_categories.id", ondelete="SET NULL"), nullable=True)
    item_type = Column(String(16), nullable=False)
    upstream_item_id = Column(String(64), nullable=False)
    title = Column(String(500), nullable=False)
    sort_title = Column(String(500), nullable=True)
    release_date = Column(String(64), nullable=True)
    year = Column(String(16), nullable=True)
    rating = Column(String(64), nullable=True)
    poster_url = Column(Text, nullable=True)
    container_extension = Column(String(32), nullable=True)
    direct_source = Column(Text, nullable=True)
    added = Column(String(64), nullable=True)
    summary_json = Column(Text, nullable=True)
    stream_probe_at = Column(DateTime, nullable=True)
    stream_probe_details = Column(Text, nullable=True)

    playlist = relationship("Playlist")
    category = relationship("XcVodCategory")

    def __repr__(self):
        return "<XcVodItem {}>".format(self.id)


class XcVodMetadataCache(Base):
    __tablename__ = "xc_vod_metadata_cache"
    __table_args__ = (
        UniqueConstraint("playlist_id", "action", "upstream_item_id", name="uq_xc_vod_metadata_cache_lookup"),
        Index("ix_xc_vod_metadata_cache_playlist_id", "playlist_id"),
        Index("ix_xc_vod_metadata_cache_action", "action"),
        Index("ix_xc_vod_metadata_cache_upstream_item_id", "upstream_item_id"),
        Index("ix_xc_vod_metadata_cache_expires_at", "expires_at"),
        Index("ix_xc_vod_metadata_cache_last_requested_at", "last_requested_at"),
    )
    id = Column(Integer, primary_key=True)

    playlist_id = Column(Integer, ForeignKey("playlists.id", ondelete="CASCADE"), nullable=False)
    action = Column(String(32), nullable=False)
    upstream_item_id = Column(String(128), nullable=False)
    payload_json = Column(Text, nullable=False)
    last_requested_at = Column(DateTime, nullable=False, default=func.now())
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())

    playlist = relationship("Playlist")

    def __repr__(self):
        return "<XcVodMetadataCache {}>".format(self.id)


class VodCategory(Base):
    __tablename__ = "vod_categories"
    __table_args__ = (
        Index("ix_vod_categories_content_type", "content_type"),
        Index("ix_vod_categories_name", "name"),
        UniqueConstraint("content_type", "name", name="uq_vod_category_content_type_name"),
    )
    id = Column(Integer, primary_key=True)

    content_type = Column(String(16), nullable=False)
    name = Column(String(500), nullable=False)
    sort_order = Column(Integer, nullable=False, default=0)
    enabled = Column(Boolean, nullable=False, default=True)
    profile_id = Column(String(64), nullable=True)
    generate_strm_files = Column(Boolean, nullable=False, default=False)
    expose_http_library = Column(Boolean, nullable=False, default=False)
    strm_base_url = Column(String(1024), nullable=True)
    content_title_rules = Column(Text, nullable=True)

    xc_category_links = relationship("VodCategoryXcCategory", back_populates="category", cascade="all, delete-orphan")
    item_cache = relationship("VodCategoryItem", back_populates="category", cascade="all, delete-orphan")

    def __repr__(self):
        return "<VodCategory {}>".format(self.id)


class VodCategoryXcCategory(Base):
    __tablename__ = "vod_category_xc_categories"
    __table_args__ = (
        Index("ix_vod_category_xc_categories_category_id", "category_id"),
        Index("ix_vod_category_xc_categories_xc_category_id", "xc_category_id"),
        UniqueConstraint("category_id", "xc_category_id", name="uq_vod_category_xc_category"),
    )
    id = Column(Integer, primary_key=True)

    category_id = Column(Integer, ForeignKey("vod_categories.id", ondelete="CASCADE"), nullable=False)
    xc_category_id = Column(Integer, ForeignKey("xc_vod_categories.id", ondelete="CASCADE"), nullable=False)
    priority = Column(Integer, nullable=False, default=0, server_default="0")
    strip_title_prefixes = Column(Text, nullable=True)
    strip_title_suffixes = Column(Text, nullable=True)

    category = relationship("VodCategory", back_populates="xc_category_links")
    xc_category = relationship("XcVodCategory")

    def __repr__(self):
        return "<VodCategoryXcCategory {}>".format(self.id)


class VodCategoryItem(Base):
    __tablename__ = "vod_category_items"
    __table_args__ = (
        Index("ix_vod_category_items_category_id", "category_id"),
        Index("ix_vod_category_items_item_type", "item_type"),
        Index("ix_vod_category_items_dedupe_key", "dedupe_key"),
        Index("ix_vod_category_items_title", "title"),
        Index("ix_vod_category_items_sort_title", "sort_title"),
        UniqueConstraint("category_id", "dedupe_key", name="uq_vod_category_item_dedupe"),
    )
    id = Column(Integer, primary_key=True)

    category_id = Column(Integer, ForeignKey("vod_categories.id", ondelete="CASCADE"), nullable=False)
    item_type = Column(String(16), nullable=False)
    dedupe_key = Column(String(512), nullable=False)
    title = Column(String(500), nullable=False)
    sort_title = Column(String(500), nullable=True)
    release_date = Column(String(64), nullable=True)
    year = Column(String(16), nullable=True)
    rating = Column(String(64), nullable=True)
    poster_url = Column(Text, nullable=True)
    container_extension = Column(String(32), nullable=True)
    summary_json = Column(Text, nullable=True)

    category = relationship("VodCategory", back_populates="item_cache")
    source_links = relationship("VodCategoryItemSource", back_populates="category_item", cascade="all, delete-orphan")
    episode_cache = relationship("VodCategoryEpisode", back_populates="category_item", cascade="all, delete-orphan")

    def __repr__(self):
        return "<VodCategoryItem {}>".format(self.id)


class VodCategoryItemSource(Base):
    __tablename__ = "vod_category_item_sources"
    __table_args__ = (
        Index("ix_vod_category_item_sources_category_item_id", "category_item_id"),
        Index("ix_vod_category_item_sources_source_item_id", "source_item_id"),
        UniqueConstraint("category_item_id", "source_item_id", name="uq_vod_category_item_source"),
    )
    id = Column(Integer, primary_key=True)

    category_item_id = Column(Integer, ForeignKey("vod_category_items.id", ondelete="CASCADE"), nullable=False)
    source_item_id = Column(Integer, ForeignKey("xc_vod_items.id", ondelete="CASCADE"), nullable=False)

    category_item = relationship("VodCategoryItem", back_populates="source_links")
    source_item = relationship("XcVodItem")

    def __repr__(self):
        return "<VodCategoryItemSource {}>".format(self.id)


class VodCategoryEpisode(Base):
    __tablename__ = "vod_category_episodes"
    __table_args__ = (
        Index("ix_vod_category_episodes_category_item_id", "category_item_id"),
        Index("ix_vod_category_episodes_dedupe_key", "dedupe_key"),
        UniqueConstraint("category_item_id", "dedupe_key", name="uq_vod_category_episode_dedupe"),
    )
    id = Column(Integer, primary_key=True)

    category_item_id = Column(Integer, ForeignKey("vod_category_items.id", ondelete="CASCADE"), nullable=False)
    dedupe_key = Column(String(512), nullable=False)
    season_number = Column(Integer, nullable=True)
    episode_number = Column(Integer, nullable=True)
    title = Column(String(500), nullable=True)
    container_extension = Column(String(32), nullable=True)
    summary_json = Column(Text, nullable=True)
    stream_probe_at = Column(DateTime, nullable=True)
    stream_probe_details = Column(Text, nullable=True)

    category_item = relationship("VodCategoryItem", back_populates="episode_cache")
    source_links = relationship("VodCategoryEpisodeSource", back_populates="episode", cascade="all, delete-orphan")

    def __repr__(self):
        return "<VodCategoryEpisode {}>".format(self.id)


class VodCategoryEpisodeSource(Base):
    __tablename__ = "vod_category_episode_sources"
    __table_args__ = (
        Index("ix_vod_category_episode_sources_episode_id", "episode_id"),
        Index("ix_vod_category_episode_sources_category_item_source_id", "category_item_source_id"),
        Index("ix_vod_category_episode_sources_upstream_episode_id", "upstream_episode_id"),
        UniqueConstraint(
            "episode_id", "category_item_source_id", "upstream_episode_id", name="uq_vod_category_episode_source"
        ),
    )
    id = Column(Integer, primary_key=True)

    episode_id = Column(Integer, ForeignKey("vod_category_episodes.id", ondelete="CASCADE"), nullable=False)
    category_item_source_id = Column(
        Integer, ForeignKey("vod_category_item_sources.id", ondelete="CASCADE"), nullable=False
    )
    upstream_episode_id = Column(String(128), nullable=False)
    season_number = Column(Integer, nullable=True)
    episode_number = Column(Integer, nullable=True)
    title = Column(String(500), nullable=True)
    container_extension = Column(String(32), nullable=True)
    summary_json = Column(Text, nullable=True)

    episode = relationship("VodCategoryEpisode", back_populates="source_links")
    category_item_source = relationship("VodCategoryItemSource")

    def __repr__(self):
        return "<VodCategoryEpisodeSource {}>".format(self.id)


channels_tags_association_table = Table(
    "channels_tags_group",
    Base.metadata,
    Column("channel_id", Integer, ForeignKey("channels.id")),
    Column("tag_id", Integer, ForeignKey("channel_tags.id")),
)


class Channel(Base):
    __tablename__ = "channels"
    __table_args__ = (
        Index("ix_channels_name", "name"),
        Index("ix_channels_number", "number"),
        Index("ix_channels_tvh_uuid", "tvh_uuid"),
    )
    id = Column(Integer, primary_key=True)

    enabled = Column(Boolean, nullable=False)
    channel_type = Column(String(32), nullable=False, default="standard", server_default="standard")
    name = Column(String(500))
    logo_url = Column(Text)
    logo_base64 = Column(Text)
    number = Column(Integer)
    tvh_uuid = Column(String(500))
    cso_enabled = Column(Boolean, nullable=False, default=False)
    cso_policy = Column(Text)
    vod_schedule_mode = Column(String(32), nullable=True)
    vod_schedule_direction = Column(String(8), nullable=True)

    # Link with a guide
    guide_id = Column(Integer, ForeignKey("epgs.id"))
    guide_name = Column(String(256))
    guide_channel_id = Column(String(64))
    guide_offset_minutes = Column(Integer, nullable=False, default=0)

    guide = relationship("Epg", back_populates="channels")

    # Backref to all associated linked sources
    sources = relationship("ChannelSource", back_populates="channel", cascade="all, delete-orphan")
    vod_channel_rules = relationship("VodChannelRule", back_populates="channel", cascade="all, delete-orphan")
    suggestions = relationship("ChannelSuggestion", back_populates="channel", cascade="all, delete-orphan")
    recording_rules = relationship("RecordingRule", back_populates="channel", cascade="all, delete-orphan")
    recordings = relationship("Recording", back_populates="channel", cascade="all, delete-orphan")

    # Specify many-to-many relationships
    tags = relationship("ChannelTag", secondary=channels_tags_association_table)

    def __repr__(self):
        return "<Channel {}>".format(self.id)


class ChannelTag(Base):
    __tablename__ = "channel_tags"
    __table_args__ = (Index("ix_channel_tags_name", "name", unique=True),)
    id = Column(Integer, primary_key=True)

    name = Column(String(64))

    def __repr__(self):
        return "<ChannelTag {}>".format(self.id)


class VodChannelRule(Base):
    __tablename__ = "vod_channel_rules"
    __table_args__ = (Index("ix_vod_channel_rules_channel_id", "channel_id"),)
    id = Column(Integer, primary_key=True)

    channel_id = Column(Integer, ForeignKey("channels.id", ondelete="CASCADE"), nullable=False)
    position = Column(Integer, nullable=False, default=0, server_default="0")
    operator = Column(String(16), nullable=False, default="include", server_default="include")
    rule_type = Column(String(64), nullable=False)
    value = Column(Text, nullable=True)
    enabled = Column(Boolean, nullable=False, default=True, server_default="true")

    channel = relationship("Channel", back_populates="vod_channel_rules")

    def __repr__(self):
        return "<VodChannelRule {}>".format(self.id)


class ChannelSource(Base):
    __tablename__ = "channel_sources"
    __table_args__ = (
        Index("ix_channel_sources_playlist_stream_name", "playlist_stream_name"),
        Index("ix_channel_sources_priority", "priority"),
        Index("ix_channel_sources_tvh_uuid", "tvh_uuid"),
    )
    id = Column(Integer, primary_key=True)

    # Link with channel
    channel_id = Column(Integer, ForeignKey("channels.id"), nullable=False)

    # Link with a playlist
    playlist_id = Column(Integer, ForeignKey("playlists.id"), nullable=True)
    xc_account_id = Column(Integer, ForeignKey("xc_accounts.id"), nullable=True)
    playlist_stream_name = Column(String(500))
    playlist_stream_url = Column(Text)
    use_hls_proxy = Column(Boolean, nullable=False, default=False)
    auto_update = Column(Boolean, nullable=False, default=False)
    last_health_check_at = Column(DateTime, nullable=True)
    last_health_check_status = Column(String(32), nullable=True)
    last_health_check_reason = Column(String(64), nullable=True)
    last_health_check_metrics = Column(Text, nullable=True)
    stream_probe_at = Column(DateTime, nullable=True)
    stream_probe_details = Column(Text, nullable=True)
    priority = Column(Integer, nullable=False, default=0, server_default="0")
    tvh_uuid = Column(String(500))

    channel = relationship("Channel", back_populates="sources")
    playlist = relationship("Playlist", back_populates="channel_sources")
    xc_account = relationship("XcAccount")

    def __repr__(self):
        return "<ChannelSource {}>".format(self.id)


class ChannelSuggestion(Base):
    __tablename__ = "channel_suggestions"
    __table_args__ = (
        Index("ix_channel_suggestions_channel_id", "channel_id"),
        Index("ix_channel_suggestions_playlist_id", "playlist_id"),
        Index("ix_channel_suggestions_stream_id", "stream_id"),
        Index("ix_channel_suggestions_stream_name", "stream_name"),
        Index("ix_channel_suggestions_group_title", "group_title"),
        Index("ix_channel_suggestions_playlist_name", "playlist_name"),
        Index("ix_channel_suggestions_source_type", "source_type"),
        UniqueConstraint("channel_id", "playlist_id", "stream_id", name="uq_channel_suggestion_stream"),
    )
    id = Column(Integer, primary_key=True)

    channel_id = Column(Integer, ForeignKey("channels.id", ondelete="CASCADE"), nullable=False)
    playlist_id = Column(Integer, ForeignKey("playlists.id", ondelete="CASCADE"), nullable=False)
    stream_id = Column(Integer, nullable=False)
    stream_name = Column(String(500))
    stream_url = Column(Text)
    group_title = Column(String(500))
    playlist_name = Column(String(500))
    source_type = Column(String(16), default="M3U")
    score = Column(Float, nullable=False, default=0)
    dismissed = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    channel = relationship("Channel", back_populates="suggestions")
    playlist = relationship("Playlist", back_populates="suggestions")

    def __repr__(self):
        return "<ChannelSuggestion {}>".format(self.id)


class RecordingRule(Base):
    __tablename__ = "recording_rules"
    __table_args__ = (
        Index("ix_recording_rules_owner_user_id", "owner_user_id"),
        Index("ix_recording_rules_title_match", "title_match"),
    )
    id = Column(Integer, primary_key=True)

    channel_id = Column(Integer, ForeignKey("channels.id"), nullable=False)
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    recording_profile_key = Column(String(64), nullable=False, default="default")
    title_match = Column(String(500))
    enabled = Column(Boolean, nullable=False, default=True)
    lookahead_days = Column(Integer, nullable=False, default=7)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    # Backref to channel
    channel = relationship("Channel", back_populates="recording_rules")
    recordings = relationship("Recording", back_populates="rule", cascade="all, delete-orphan")

    def __repr__(self):
        return "<RecordingRule {}>".format(self.id)


class Recording(Base):
    __tablename__ = "recordings"
    __table_args__ = (
        Index("ix_recordings_owner_user_id", "owner_user_id"),
        Index("ix_recordings_title", "title"),
        Index("ix_recordings_start_ts", "start_ts"),
        Index("ix_recordings_stop_ts", "stop_ts"),
        Index("ix_recordings_status", "status"),
        Index("ix_recordings_sync_status", "sync_status"),
        Index("ix_recordings_tvh_uuid", "tvh_uuid"),
    )
    id = Column(Integer, primary_key=True)

    channel_id = Column(Integer, ForeignKey("channels.id"), nullable=False)
    rule_id = Column(Integer, ForeignKey("recording_rules.id"), nullable=True)
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    recording_profile_key = Column(String(64), nullable=False, default="default")
    epg_programme_id = Column(Integer, nullable=True)

    title = Column(String(500))
    description = Column(String(2000))
    start_ts = Column(Integer)
    stop_ts = Column(Integer)

    status = Column(String(32), default="scheduled")
    sync_status = Column(String(32), default="pending")
    sync_error = Column(String(1024))
    tvh_uuid = Column(String(128))

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    channel = relationship("Channel", back_populates="recordings")
    rule = relationship("RecordingRule", back_populates="recordings")

    def __repr__(self):
        return "<Recording {}>".format(self.id)


user_roles_association_table = Table(
    "user_roles",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("users.id"), nullable=False),
    Column("role_id", Integer, ForeignKey("roles.id"), nullable=False),
)


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        Index("ix_users_username", "username", unique=True),
        Index("ix_users_oidc_issuer_subject", "oidc_issuer", "oidc_subject", unique=True),
        Index("ix_users_streaming_key", "streaming_key", unique=True),
        Index("ix_users_streaming_key_hash", "streaming_key_hash", unique=True),
    )
    id = Column(Integer, primary_key=True)

    username = Column(String(64), nullable=False)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    auth_source = Column(String(32), nullable=False, default="local")
    oidc_issuer = Column(String(255), nullable=True)
    oidc_subject = Column(String(255), nullable=True)
    oidc_email = Column(String(255), nullable=True)

    streaming_key = Column(String(255), nullable=True)
    streaming_key_hash = Column(String(255), nullable=True)
    streaming_key_created_at = Column(DateTime, nullable=True)
    tvh_sync_status = Column(String(32), nullable=True)
    tvh_sync_error = Column(String(1024), nullable=True)
    tvh_sync_updated_at = Column(DateTime, nullable=True)
    dvr_access_mode = Column(String(32), nullable=False, default="none")
    dvr_retention_policy = Column(String(32), nullable=False, default="forever")
    timeshift_enabled = Column(Boolean, nullable=False, default=False)
    vod_access_mode = Column(String(32), nullable=False, default="none")
    vod_generate_strm_files = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    last_login_at = Column(DateTime, nullable=True)
    last_stream_key_used_at = Column(DateTime, nullable=True)

    roles = relationship("Role", secondary=user_roles_association_table, back_populates="users")
    sessions = relationship("UserSession", back_populates="user", cascade="all, delete-orphan")
    stream_audits = relationship("StreamAuditLog", back_populates="user", cascade="all, delete-orphan")

    def __repr__(self):
        return "<User {}>".format(self.id)


class Role(Base):
    __tablename__ = "roles"
    __table_args__ = (Index("ix_roles_name", "name", unique=True),)
    id = Column(Integer, primary_key=True)

    name = Column(String(32), nullable=False)
    description = Column(String(255), nullable=True)

    users = relationship("User", secondary=user_roles_association_table, back_populates="roles")

    def __repr__(self):
        return "<Role {}>".format(self.id)


class UserSession(Base):
    __tablename__ = "user_sessions"
    __table_args__ = (
        Index("ix_user_sessions_user_id", "user_id"),
        Index("ix_user_sessions_token_hash", "token_hash", unique=True),
    )
    id = Column(Integer, primary_key=True)

    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    token_hash = Column(String(128), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    last_used_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=True)
    revoked = Column(Boolean, nullable=False, default=False)
    user_agent = Column(String(255), nullable=True)
    ip_address = Column(String(64), nullable=True)

    user = relationship("User", back_populates="sessions")

    def __repr__(self):
        return "<UserSession {}>".format(self.id)


class StreamAuditLog(Base):
    __tablename__ = "stream_audit_logs"
    __table_args__ = (
        Index("ix_stream_audit_logs_user_id", "user_id"),
        Index("ix_stream_audit_logs_event_type", "event_type"),
    )
    id = Column(Integer, primary_key=True)

    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    event_type = Column(String(64), nullable=False)
    severity = Column(String(16), nullable=False, default="info", server_default="info")
    endpoint = Column(Text, nullable=True)
    ip_address = Column(String(64), nullable=True)
    user_agent = Column(Text, nullable=True)
    details = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    user = relationship("User", back_populates="stream_audits")

    def __repr__(self):
        return "<StreamAuditLog {}>".format(self.id)


class CsoEventLog(Base):
    __tablename__ = "cso_event_logs"
    __table_args__ = (
        Index("ix_cso_event_logs_channel_id", "channel_id"),
        Index("ix_cso_event_logs_source_id", "source_id"),
        Index("ix_cso_event_logs_playlist_id", "playlist_id"),
        Index("ix_cso_event_logs_recording_id", "recording_id"),
        Index("ix_cso_event_logs_vod_category_id", "vod_category_id"),
        Index("ix_cso_event_logs_vod_item_id", "vod_item_id"),
        Index("ix_cso_event_logs_vod_episode_id", "vod_episode_id"),
        Index("ix_cso_event_logs_tvh_subscription_id", "tvh_subscription_id"),
        Index("ix_cso_event_logs_session_id", "session_id"),
        Index("ix_cso_event_logs_event_type", "event_type"),
        Index("ix_cso_event_logs_channel_created", "channel_id", "created_at"),
        Index("ix_cso_event_logs_event_type_created", "event_type", "created_at"),
    )

    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("channels.id", ondelete="SET NULL"), nullable=True)
    source_id = Column(Integer, ForeignKey("channel_sources.id", ondelete="SET NULL"), nullable=True)
    playlist_id = Column(Integer, ForeignKey("playlists.id", ondelete="SET NULL"), nullable=True)
    recording_id = Column(Integer, ForeignKey("recordings.id", ondelete="SET NULL"), nullable=True)
    vod_category_id = Column(Integer, ForeignKey("vod_categories.id", ondelete="SET NULL"), nullable=True)
    vod_item_id = Column(Integer, ForeignKey("vod_category_items.id", ondelete="SET NULL"), nullable=True)
    vod_episode_id = Column(Integer, ForeignKey("vod_category_episodes.id", ondelete="SET NULL"), nullable=True)
    tvh_subscription_id = Column(String(128), nullable=True)
    session_id = Column(String(128), nullable=True)
    event_type = Column(String(64), nullable=False)
    severity = Column(String(16), nullable=False, default="info")
    details_json = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    channel = relationship("Channel")
    source = relationship("ChannelSource")
    playlist = relationship("Playlist")
    recording = relationship("Recording")
    vod_category = relationship("VodCategory")
    vod_item = relationship("VodCategoryItem")
    vod_episode = relationship("VodCategoryEpisode")

    def __repr__(self):
        return "<CsoEventLog {}>".format(self.id)
