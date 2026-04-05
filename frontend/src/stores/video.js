import {defineStore} from 'pinia';

const DEFAULT_SIZE = {width: 360, height: 203};
const DEFAULT_POSITION = {right: 24, bottom: 24};
const STORAGE_KEY = 'tic_video_player_state';
const LOADED_STATE = loadState();

function loadState() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') {
      return {};
    }
    return parsed;
  } catch (error) {
    console.warn('Failed to load video player state:', error);
    return {};
  }
}

function persistState(state) {
  try {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        size: state.size,
        position: state.position,
        volume: state.volume,
      }),
    );
  } catch (error) {
    console.warn('Failed to persist video player state:', error);
  }
}

export const useVideoStore = defineStore('video', {
  state: () => ({
    isVisible: false,
    streamUrl: null,
    streamTitle: null,
    streamType: 'auto',
    streamCandidates: [],
    activeCandidateIndex: 0,
    seekMode: 'native',
    playbackProfiles: [],
    selectedPlaybackProfile: null,
    previewMetadataUrl: null,
    sourceResolution: null,
    durationSeconds: null,
    size: LOADED_STATE.size || DEFAULT_SIZE,
    position: LOADED_STATE.position || DEFAULT_POSITION,
    volume: typeof LOADED_STATE.volume === 'number' ? LOADED_STATE.volume : 1,
  }),
  actions: {
    showPlayer({
                 url,
                 candidates = [],
                 title = null,
                 type = 'auto',
                 seekMode = 'native',
                 playbackProfiles = [],
                 selectedPlaybackProfile = null,
                 previewMetadataUrl = null,
                 sourceResolution = null,
                 durationSeconds = null,
               }) {
      const nextCandidates = Array.isArray(candidates) ?
        candidates.filter((candidate) => candidate?.url) :
        [];
      const primaryCandidate = nextCandidates[0] || null;
      this.streamCandidates = nextCandidates;
      this.activeCandidateIndex = 0;
      this.streamUrl = primaryCandidate?.url || url;
      this.streamTitle = title;
      this.streamType = primaryCandidate?.streamType || type;
      this.seekMode = seekMode || 'native';
      this.playbackProfiles = Array.isArray(playbackProfiles) ?
        playbackProfiles :
        [];
      this.selectedPlaybackProfile = selectedPlaybackProfile ||
        this.playbackProfiles[0]?.id || null;
      this.previewMetadataUrl = previewMetadataUrl || null;
      this.sourceResolution = sourceResolution || null;
      this.durationSeconds = typeof durationSeconds === 'number' &&
      Number.isFinite(durationSeconds) ? durationSeconds : null;
      this.isVisible = true;
    },
    hidePlayer() {
      this.isVisible = false;
      this.streamUrl = null;
      this.streamTitle = null;
      this.streamType = 'auto';
      this.streamCandidates = [];
      this.activeCandidateIndex = 0;
      this.seekMode = 'native';
      this.playbackProfiles = [];
      this.selectedPlaybackProfile = null;
      this.previewMetadataUrl = null;
      this.sourceResolution = null;
      this.durationSeconds = null;
    },
    setPlaybackProfile(profileId, url, type = null, seekMode = null) {
      this.selectedPlaybackProfile = profileId || null;
      if (url) {
        this.streamUrl = url;
      }
      if (type) {
        this.streamType = type;
      }
      if (seekMode) {
        this.seekMode = seekMode;
      }
    },
    setActiveCandidate(index) {
      const nextIndex = Number(index);
      if (!Number.isInteger(nextIndex) || nextIndex < 0 || nextIndex >=
        this.streamCandidates.length) {
        return;
      }
      const candidate = this.streamCandidates[nextIndex];
      if (!candidate?.url) {
        return;
      }
      this.activeCandidateIndex = nextIndex;
      this.streamUrl = candidate.url;
      this.streamType = candidate.streamType || this.streamType || 'auto';
    },
    setVodMetadata({
                     sourceResolution = null,
                     durationSeconds = null,
                     playbackProfiles = null,
                   }) {
      if (sourceResolution) {
        this.sourceResolution = sourceResolution;
      }
      if (typeof durationSeconds === 'number' &&
        Number.isFinite(durationSeconds) && durationSeconds > 0) {
        this.durationSeconds = durationSeconds;
      }
      if (Array.isArray(playbackProfiles) && playbackProfiles.length) {
        this.playbackProfiles = playbackProfiles;
        if (!this.playbackProfiles.some(
          (profile) => profile.id === this.selectedPlaybackProfile)) {
          this.selectedPlaybackProfile = this.playbackProfiles[0]?.id || null;
        }
      }
    },
    setSize(size) {
      this.size = size;
      persistState(this);
    },
    setPosition(position) {
      this.position = position;
      persistState(this);
    },
    setVolume(volume) {
      const volume_number = Number(volume);
      const parsed = Number.isFinite(volume_number) ?
        Math.min(1, Math.max(0, volume_number)) :
        1;
      this.volume = parsed;
      persistState(this);
    },
  },
});
