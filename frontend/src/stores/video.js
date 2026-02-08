import {defineStore} from 'pinia';

const DEFAULT_SIZE = {width: 360, height: 203};
const DEFAULT_POSITION = {right: 24, bottom: 24};
const STORAGE_KEY = 'tic_video_player_state';

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
    size: loadState().size || DEFAULT_SIZE,
    position: loadState().position || DEFAULT_POSITION,
    volume: typeof loadState().volume === 'number' ? loadState().volume : 1,
  }),
  actions: {
    showPlayer({url, title = null, type = 'auto'}) {
      this.streamUrl = url;
      this.streamTitle = title;
      this.streamType = type;
      this.isVisible = true;
    },
    hidePlayer() {
      this.isVisible = false;
      this.streamUrl = null;
      this.streamTitle = null;
      this.streamType = 'auto';
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
      this.volume = volume;
      persistState(this);
    },
  },
});
