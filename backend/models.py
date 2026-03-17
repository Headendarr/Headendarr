#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Boolean,
    Table,
    MetaData,
    DateTime,
    func,
    Text,
    Float,
    UniqueConstraint,
    Index,
)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import relationship, sessionmaker, declarative_base

from backend import config

metadata = MetaData()
Base = declarative_base(metadata=metadata)

engine = create_async_engine(config.sqlalchemy_database_async_uri, echo=config.enable_sqlalchemy_debugging)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Use of 'db' in this project is now deprecated and will be removed in a future release. Use Session instead.
db = SQLAlchemy()


class Epg(Base):
    __tablename__ = "epgs"
    id = Column(Integer, primary_key=True)

    enabled = Column(Boolean, nullable=False, unique=False)
    name = Column(String(500), index=True, unique=False)
    url = Column(Text, index=False, unique=False)
    user_agent = Column(String(255), nullable=True)
    update_schedule = Column(String(16), nullable=False, default="12h")

    # Backref to all associated linked channels
    epg_channels = relationship("EpgChannels", back_populates="guide", cascade="all, delete-orphan")
    channels = relationship("Channel", back_populates="guide", cascade="all, delete-orphan")

    def __repr__(self):
        return "<Epg {}>".format(self.id)


class EpgChannels(Base):
    __tablename__ = "epg_channels"
    __table_args__ = (Index("ix_epg_channels_epg_id_channel_id", "epg_id", "channel_id"),)
    id = Column(Integer, primary_key=True)

    channel_id = Column(String(256), index=True, unique=False)
    name = Column(String(500), index=True, unique=False)
    icon_url = Column(Text, index=False, unique=False)

    # Link with an epg
    epg_id = Column(Integer, ForeignKey("epgs.id"), nullable=False, index=True)

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
    __table_args__ = (Index("ix_epg_channel_programmes_epg_channel_id_start", "epg_channel_id", "start"),)
    id = Column(Integer, primary_key=True)

    channel_id = Column(Text, index=True, unique=False)
    title = Column(Text, index=True, unique=False)
    sub_title = Column(Text, index=False, unique=False)
    desc = Column(Text, index=False, unique=False)
    series_desc = Column(Text, index=False, unique=False)
    country = Column(Text, index=False, unique=False)
    icon_url = Column(Text, index=False, unique=False)
    start = Column(Text, index=False, unique=False)
    stop = Column(Text, index=False, unique=False)
    start_timestamp = Column(Text, index=False, unique=False)
    stop_timestamp = Column(Text, index=False, unique=False)
    categories = Column(Text, index=True, unique=False)
    # Extended optional XMLTV / TVHeadend supported metadata (all nullable / optional)
    summary = Column(Text, index=False, unique=False)
    keywords = Column(Text, index=False, unique=False)  # JSON encoded list of keyword strings
    credits_json = Column(Text, index=False, unique=False)  # JSON: {"actor":[],"director":[],...}
    video_colour = Column(Text, index=False, unique=False)
    video_aspect = Column(Text, index=False, unique=False)
    video_quality = Column(Text, index=False, unique=False)
    subtitles_type = Column(Text, index=False, unique=False)
    audio_described = Column(Boolean, nullable=True)  # True -> <audio-described />
    previously_shown_date = Column(Text, index=False, unique=False)  # YYYY-MM-DD
    premiere = Column(Boolean, nullable=True)
    is_new = Column(Boolean, nullable=True)
    epnum_onscreen = Column(Text, index=False, unique=False)
    epnum_xmltv_ns = Column(Text, index=False, unique=False)
    epnum_dd_progid = Column(Text, index=False, unique=False)
    star_rating = Column(Text, index=False, unique=False)  # e.g. "3/5"
    production_year = Column(Text, index=False, unique=False)  # <date>
    rating_system = Column(Text, index=False, unique=False)
    rating_value = Column(Text, index=False, unique=False)
    metadata_lookup_hash = Column(String(64), index=True, unique=False)

    # Link with an epg channel
    epg_channel_id = Column(Integer, ForeignKey("epg_channels.id"), nullable=False, index=True)

    channel = relationship("EpgChannels", back_populates="epg_channel_programmes")

    def __repr__(self):
        return "<EpgChannelProgrammes {}>".format(self.id)


class EpgProgrammeMetadataCache(Base):
    __tablename__ = "epg_programme_metadata_cache"
    __table_args__ = (
        Index("ix_epg_programme_metadata_cache_expires_at", "expires_at"),
        Index("ix_epg_programme_metadata_cache_match_status", "match_status"),
    )

    id = Column(Integer, primary_key=True)
    lookup_hash = Column(String(64), index=True, unique=True, nullable=False)
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
    id = Column(Integer, primary_key=True)

    enabled = Column(Boolean, nullable=False, unique=False)
    connections = Column(Integer, nullable=False, unique=False)
    name = Column(String(500), index=True, unique=False)
    tvh_uuid = Column(String(64), index=True, unique=True)
    url = Column(Text, index=False, unique=False)
    account_type = Column(String(16), nullable=False, unique=False, default="M3U")
    xc_username = Column(String(255), nullable=True, unique=False)
    xc_password = Column(String(255), nullable=True, unique=False)
    use_hls_proxy = Column(Boolean, nullable=False, unique=False)
    use_custom_hls_proxy = Column(Boolean, nullable=False, unique=False)
    hls_proxy_path = Column(String(256), unique=False)
    chain_custom_hls_proxy = Column(Boolean, nullable=False, unique=False, default=False)
    hls_proxy_use_ffmpeg = Column(Boolean, nullable=False, unique=False, default=False)
    hls_proxy_prebuffer = Column(String(32), nullable=True, unique=False, default="1M")
    hls_proxy_headers = Column(Text, nullable=True, unique=False)
    user_agent = Column(String(255), nullable=True)
    xc_live_stream_format = Column(String(8), nullable=False, unique=False, default="ts")
    update_schedule = Column(String(16), nullable=False, default="off")

    # Backref to all associated linked sources
    channel_sources = relationship("ChannelSource", back_populates="playlist", cascade="all, delete-orphan")
    playlist_streams = relationship("PlaylistStreams", back_populates="playlist", cascade="all, delete-orphan")
    xc_accounts = relationship("XcAccount", back_populates="playlist", cascade="all, delete-orphan")

    def __repr__(self):
        return "<Playlist {}>".format(self.id)


class PlaylistStreams(Base):
    __tablename__ = "playlist_streams"
    __table_args__ = (Index("ix_playlist_streams_playlist_id_url_hash", "playlist_id", "url_hash"),)
    id = Column(Integer, primary_key=True)

    name = Column(String(500), index=True, unique=False)
    url = Column(Text, index=False, unique=False)
    url_hash = Column(String(32), index=False, unique=False)
    channel_id = Column(Text, index=True, unique=False)
    group_title = Column(String(500), index=True, unique=False)
    tvg_chno = Column(Integer, index=False, unique=False)
    tvg_id = Column(String(500), index=True, unique=False)
    tvg_logo = Column(Text, index=False, unique=False)
    source_type = Column(String(16), index=True, unique=False, default="M3U")
    xc_stream_id = Column(Integer, index=True, unique=False)
    xc_category_id = Column(Integer, index=True, unique=False)

    # Link with a playlist
    playlist_id = Column(Integer, ForeignKey("playlists.id"), nullable=True, index=True)

    playlist = relationship("Playlist", back_populates="playlist_streams")

    def __repr__(self):
        return "<PlaylistStreams {}>".format(self.id)


class XcAccount(Base):
    __tablename__ = "xc_accounts"
    id = Column(Integer, primary_key=True)

    playlist_id = Column(Integer, ForeignKey("playlists.id"), nullable=False)
    username = Column(String(255), nullable=False)
    password = Column(String(255), nullable=False)
    enabled = Column(Boolean, nullable=False, unique=False, default=True)
    connection_limit = Column(Integer, nullable=False, unique=False, default=1)
    label = Column(String(255), nullable=True)
    tvh_uuid = Column(String(64), index=True, unique=True)

    playlist = relationship("Playlist", back_populates="xc_accounts")

    def __repr__(self):
        return "<XcAccount {}>".format(self.id)


class XcVodCategory(Base):
    __tablename__ = "xc_vod_categories"
    __table_args__ = (
        UniqueConstraint("playlist_id", "category_type", "upstream_category_id", name="uq_xc_vod_category_upstream"),
    )
    id = Column(Integer, primary_key=True)

    playlist_id = Column(Integer, ForeignKey("playlists.id"), nullable=False, index=True)
    category_type = Column(String(16), nullable=False, index=True)
    upstream_category_id = Column(String(64), nullable=False, index=True)
    name = Column(String(500), nullable=False, index=True)
    parent_id = Column(String(64), nullable=True)

    playlist = relationship("Playlist")

    def __repr__(self):
        return "<XcVodCategory {}>".format(self.id)


class XcVodItem(Base):
    __tablename__ = "xc_vod_items"
    __table_args__ = (UniqueConstraint("playlist_id", "item_type", "upstream_item_id", name="uq_xc_vod_item_upstream"),)
    id = Column(Integer, primary_key=True)

    playlist_id = Column(Integer, ForeignKey("playlists.id"), nullable=False, index=True)
    category_id = Column(Integer, ForeignKey("xc_vod_categories.id"), nullable=True, index=True)
    item_type = Column(String(16), nullable=False, index=True)
    upstream_item_id = Column(String(64), nullable=False, index=True)
    title = Column(String(500), nullable=False, index=True)
    sort_title = Column(String(500), nullable=True, index=True)
    release_date = Column(String(64), nullable=True)
    year = Column(String(16), nullable=True)
    rating = Column(String(64), nullable=True)
    poster_url = Column(Text, nullable=True)
    container_extension = Column(String(32), nullable=True)
    direct_source = Column(Text, nullable=True)
    added = Column(String(64), nullable=True)
    summary_json = Column(Text, nullable=True)
    stream_probe_at = Column(DateTime, nullable=True, unique=False)
    stream_probe_details = Column(Text, nullable=True, unique=False)

    playlist = relationship("Playlist")
    category = relationship("XcVodCategory")

    def __repr__(self):
        return "<XcVodItem {}>".format(self.id)


class XcVodMetadataCache(Base):
    __tablename__ = "xc_vod_metadata_cache"
    __table_args__ = (
        UniqueConstraint("playlist_id", "action", "upstream_item_id", name="uq_xc_vod_metadata_cache_lookup"),
        Index("ix_xc_vod_metadata_cache_expires_at", "expires_at"),
        Index("ix_xc_vod_metadata_cache_last_requested_at", "last_requested_at"),
    )
    id = Column(Integer, primary_key=True)

    playlist_id = Column(Integer, ForeignKey("playlists.id", ondelete="CASCADE"), nullable=False, index=True)
    action = Column(String(32), nullable=False, index=True)
    upstream_item_id = Column(String(128), nullable=False, index=True)
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
    __table_args__ = (UniqueConstraint("content_type", "name", name="uq_vod_category_content_type_name"),)
    id = Column(Integer, primary_key=True)

    content_type = Column(String(16), nullable=False, index=True)
    name = Column(String(500), nullable=False, index=True)
    sort_order = Column(Integer, nullable=False, default=0)
    enabled = Column(Boolean, nullable=False, default=True)
    profile_id = Column(String(64), nullable=True)
    generate_strm_files = Column(Boolean, nullable=False, default=False)
    strm_base_url = Column(String(1024), nullable=True)

    xc_category_links = relationship("VodCategoryXcCategory", back_populates="category", cascade="all, delete-orphan")
    item_cache = relationship("VodCategoryItem", back_populates="category", cascade="all, delete-orphan")

    def __repr__(self):
        return "<VodCategory {}>".format(self.id)


class VodCategoryXcCategory(Base):
    __tablename__ = "vod_category_xc_categories"
    __table_args__ = (UniqueConstraint("category_id", "xc_category_id", name="uq_vod_category_xc_category"),)
    id = Column(Integer, primary_key=True)

    category_id = Column(Integer, ForeignKey("vod_categories.id", ondelete="CASCADE"), nullable=False, index=True)
    xc_category_id = Column(Integer, ForeignKey("xc_vod_categories.id", ondelete="CASCADE"), nullable=False, index=True)
    priority = Column(Integer, nullable=False, default=0, server_default="0")
    strip_title_prefixes = Column(Text, nullable=True)
    strip_title_suffixes = Column(Text, nullable=True)

    category = relationship("VodCategory", back_populates="xc_category_links")
    xc_category = relationship("XcVodCategory")

    def __repr__(self):
        return "<VodCategoryXcCategory {}>".format(self.id)


class VodCategoryItem(Base):
    __tablename__ = "vod_category_items"
    __table_args__ = (UniqueConstraint("category_id", "dedupe_key", name="uq_vod_category_item_dedupe"),)
    id = Column(Integer, primary_key=True)

    category_id = Column(Integer, ForeignKey("vod_categories.id", ondelete="CASCADE"), nullable=False, index=True)
    item_type = Column(String(16), nullable=False, index=True)
    dedupe_key = Column(String(512), nullable=False, index=True)
    title = Column(String(500), nullable=False, index=True)
    sort_title = Column(String(500), nullable=True, index=True)
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
    __table_args__ = (UniqueConstraint("category_item_id", "source_item_id", name="uq_vod_category_item_source"),)
    id = Column(Integer, primary_key=True)

    category_item_id = Column(
        Integer, ForeignKey("vod_category_items.id", ondelete="CASCADE"), nullable=False, index=True
    )
    source_item_id = Column(Integer, ForeignKey("xc_vod_items.id", ondelete="CASCADE"), nullable=False, index=True)

    category_item = relationship("VodCategoryItem", back_populates="source_links")
    source_item = relationship("XcVodItem")

    def __repr__(self):
        return "<VodCategoryItemSource {}>".format(self.id)


class VodCategoryEpisode(Base):
    __tablename__ = "vod_category_episodes"
    __table_args__ = (UniqueConstraint("category_item_id", "dedupe_key", name="uq_vod_category_episode_dedupe"),)
    id = Column(Integer, primary_key=True)

    category_item_id = Column(
        Integer, ForeignKey("vod_category_items.id", ondelete="CASCADE"), nullable=False, index=True
    )
    dedupe_key = Column(String(512), nullable=False, index=True)
    season_number = Column(Integer, nullable=True)
    episode_number = Column(Integer, nullable=True)
    title = Column(String(500), nullable=True)
    container_extension = Column(String(32), nullable=True)
    summary_json = Column(Text, nullable=True)
    stream_probe_at = Column(DateTime, nullable=True, unique=False)
    stream_probe_details = Column(Text, nullable=True, unique=False)

    category_item = relationship("VodCategoryItem", back_populates="episode_cache")
    source_links = relationship("VodCategoryEpisodeSource", back_populates="episode", cascade="all, delete-orphan")

    def __repr__(self):
        return "<VodCategoryEpisode {}>".format(self.id)


class VodCategoryEpisodeSource(Base):
    __tablename__ = "vod_category_episode_sources"
    __table_args__ = (
        UniqueConstraint(
            "episode_id", "category_item_source_id", "upstream_episode_id", name="uq_vod_category_episode_source"
        ),
    )
    id = Column(Integer, primary_key=True)

    episode_id = Column(Integer, ForeignKey("vod_category_episodes.id", ondelete="CASCADE"), nullable=False, index=True)
    category_item_source_id = Column(
        Integer, ForeignKey("vod_category_item_sources.id", ondelete="CASCADE"), nullable=False, index=True
    )
    upstream_episode_id = Column(String(128), nullable=False, index=True)
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
    id = Column(Integer, primary_key=True)

    enabled = Column(Boolean, nullable=False, unique=False)
    name = Column(String(500), index=True, unique=False)
    logo_url = Column(Text, index=False, unique=False)
    logo_base64 = Column(Text, index=False, unique=False)
    number = Column(Integer, index=True, unique=False)
    tvh_uuid = Column(String(500), index=True, unique=False)
    cso_enabled = Column(Boolean, nullable=False, unique=False, default=False)
    cso_policy = Column(Text, index=False, unique=False)

    # Link with a guide
    guide_id = Column(Integer, ForeignKey("epgs.id"))
    guide_name = Column(String(256), index=False, unique=False)
    guide_channel_id = Column(String(64), index=False, unique=False)
    guide_offset_minutes = Column(Integer, nullable=False, unique=False, default=0)

    guide = relationship("Epg", back_populates="channels")

    # Backref to all associated linked sources
    sources = relationship("ChannelSource", back_populates="channel", cascade="all, delete-orphan")
    suggestions = relationship("ChannelSuggestion", back_populates="channel", cascade="all, delete-orphan")
    recording_rules = relationship("RecordingRule", back_populates="channel", cascade="all, delete-orphan")
    recordings = relationship("Recording", back_populates="channel", cascade="all, delete-orphan")

    # Specify many-to-many relationships
    tags = relationship("ChannelTag", secondary=channels_tags_association_table)

    def __repr__(self):
        return "<Channel {}>".format(self.id)


class ChannelTag(Base):
    __tablename__ = "channel_tags"
    id = Column(Integer, primary_key=True)

    name = Column(String(64), index=False, unique=True)

    def __repr__(self):
        return "<ChannelTag {}>".format(self.id)


class ChannelSource(Base):
    __tablename__ = "channel_sources"
    id = Column(Integer, primary_key=True)

    # Link with channel
    channel_id = Column(Integer, ForeignKey("channels.id"), nullable=False)

    # Link with a playlist
    playlist_id = Column(Integer, ForeignKey("playlists.id"), nullable=True)
    xc_account_id = Column(Integer, ForeignKey("xc_accounts.id"), nullable=True)
    playlist_stream_name = Column(String(500), index=True, unique=False)
    playlist_stream_url = Column(Text, index=False, unique=False)
    use_hls_proxy = Column(Boolean, nullable=False, unique=False, default=False)
    auto_update = Column(Boolean, nullable=False, unique=False, default=False)
    last_health_check_at = Column(DateTime, nullable=True, unique=False)
    last_health_check_status = Column(String(32), nullable=True, unique=False)
    last_health_check_reason = Column(String(64), nullable=True, unique=False)
    last_health_check_metrics = Column(Text, nullable=True, unique=False)
    stream_probe_at = Column(DateTime, nullable=True, unique=False)
    stream_probe_details = Column(Text, nullable=True, unique=False)
    priority = Column(Integer, index=True, unique=False, nullable=False, default=0, server_default="0")
    tvh_uuid = Column(String(500), index=True, unique=False)

    channel = relationship("Channel", back_populates="sources")
    playlist = relationship("Playlist", back_populates="channel_sources")
    xc_account = relationship("XcAccount")

    def __repr__(self):
        return "<ChannelSource {}>".format(self.id)


class ChannelSuggestion(Base):
    __tablename__ = "channel_suggestions"
    __table_args__ = (UniqueConstraint("channel_id", "playlist_id", "stream_id", name="uq_channel_suggestion_stream"),)
    id = Column(Integer, primary_key=True)

    channel_id = Column(Integer, ForeignKey("channels.id", ondelete="CASCADE"), nullable=False, index=True)
    playlist_id = Column(Integer, ForeignKey("playlists.id"), nullable=False, index=True)
    stream_id = Column(Integer, nullable=False, index=True)
    stream_name = Column(String(500), index=True, unique=False)
    stream_url = Column(Text, index=False, unique=False)
    group_title = Column(String(500), index=True, unique=False)
    playlist_name = Column(String(500), index=True, unique=False)
    source_type = Column(String(16), index=True, unique=False, default="M3U")
    score = Column(Float, nullable=False, default=0)
    dismissed = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    channel = relationship("Channel", back_populates="suggestions")
    playlist = relationship("Playlist")

    def __repr__(self):
        return "<ChannelSuggestion {}>".format(self.id)


class RecordingRule(Base):
    __tablename__ = "recording_rules"
    id = Column(Integer, primary_key=True)

    channel_id = Column(Integer, ForeignKey("channels.id"), nullable=False)
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    recording_profile_key = Column(String(64), nullable=False, default="default")
    title_match = Column(String(500), index=True, unique=False)
    enabled = Column(Boolean, nullable=False, unique=False, default=True)
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
    id = Column(Integer, primary_key=True)

    channel_id = Column(Integer, ForeignKey("channels.id"), nullable=False)
    rule_id = Column(Integer, ForeignKey("recording_rules.id"), nullable=True)
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    recording_profile_key = Column(String(64), nullable=False, default="default")
    epg_programme_id = Column(Integer, nullable=True)

    title = Column(String(500), index=True, unique=False)
    description = Column(String(2000), index=False, unique=False)
    start_ts = Column(Integer, index=True, unique=False)
    stop_ts = Column(Integer, index=True, unique=False)

    status = Column(String(32), index=True, unique=False, default="scheduled")
    sync_status = Column(String(32), index=True, unique=False, default="pending")
    sync_error = Column(String(1024), index=False, unique=False)
    tvh_uuid = Column(String(128), index=True, unique=False)

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
    id = Column(Integer, primary_key=True)

    username = Column(String(64), index=True, unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    auth_source = Column(String(32), nullable=False, default="local")
    oidc_issuer = Column(String(255), nullable=True)
    oidc_subject = Column(String(255), nullable=True)
    oidc_email = Column(String(255), nullable=True)

    streaming_key = Column(String(255), unique=True, nullable=True)
    streaming_key_hash = Column(String(255), unique=True, nullable=True)
    streaming_key_created_at = Column(DateTime, nullable=True)
    tvh_sync_status = Column(String(32), nullable=True)
    tvh_sync_error = Column(String(1024), nullable=True)
    tvh_sync_updated_at = Column(DateTime, nullable=True)
    dvr_access_mode = Column(String(32), nullable=False, default="none")
    dvr_retention_policy = Column(String(32), nullable=False, default="forever")
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
    id = Column(Integer, primary_key=True)

    name = Column(String(32), index=True, unique=True, nullable=False)
    description = Column(String(255), nullable=True)

    users = relationship("User", secondary=user_roles_association_table, back_populates="roles")

    def __repr__(self):
        return "<Role {}>".format(self.id)


class UserSession(Base):
    __tablename__ = "user_sessions"
    id = Column(Integer, primary_key=True)

    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    token_hash = Column(String(128), unique=True, nullable=False, index=True)
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
    id = Column(Integer, primary_key=True)

    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    event_type = Column(String(64), index=True, nullable=False)
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
        Index("ix_cso_event_logs_channel_created", "channel_id", "created_at"),
        Index("ix_cso_event_logs_event_type_created", "event_type", "created_at"),
    )

    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("channels.id", ondelete="SET NULL"), nullable=True, index=True)
    source_id = Column(Integer, ForeignKey("channel_sources.id", ondelete="SET NULL"), nullable=True, index=True)
    playlist_id = Column(Integer, ForeignKey("playlists.id", ondelete="SET NULL"), nullable=True, index=True)
    recording_id = Column(Integer, ForeignKey("recordings.id", ondelete="SET NULL"), nullable=True, index=True)
    vod_category_id = Column(Integer, ForeignKey("vod_categories.id", ondelete="SET NULL"), nullable=True, index=True)
    vod_item_id = Column(Integer, ForeignKey("vod_category_items.id", ondelete="SET NULL"), nullable=True, index=True)
    vod_episode_id = Column(
        Integer, ForeignKey("vod_category_episodes.id", ondelete="SET NULL"), nullable=True, index=True
    )
    tvh_subscription_id = Column(String(128), nullable=True, index=True)
    session_id = Column(String(128), nullable=True, index=True)
    event_type = Column(String(64), index=True, nullable=False)
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
