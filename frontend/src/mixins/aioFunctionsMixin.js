import {ref, onBeforeUnmount} from 'vue';
import {useQuasar} from 'quasar';
import axios from 'axios';

let instance;

function createAioStartupTasks() {
  const $q = useQuasar();
  const firstRun = ref(null);
  const aioMode = ref(false);
  const firstRunInitComplete = ref(false);

  let pingInterval = null;
  let runningCheckInterval = null;
  let startupFinalizing = false;

  const clearStartupIntervals = () => {
    if (pingInterval) {
      clearInterval(pingInterval);
      pingInterval = null;
    }
    if (runningCheckInterval) {
      clearInterval(runningCheckInterval);
      runningCheckInterval = null;
    }
  };

  const showStartupOverlay = (message) => {
    $q.loading.show({
      message: message || 'Waiting for TVHeadend to start for first-run setup...',
    });
  };

  const hideStartupOverlay = () => {
    $q.loading.hide();
  };

  const pingBackend = () => {
    if (!firstRun.value || startupFinalizing) {
      return;
    }
    axios({
      method: 'GET',
      url: '/tic-tvh/ping',
      timeout: 4000,
    })
      .then((response) => {
        if (response.status === 200 && response.data.includes('PONG')) {
          if (!startupFinalizing) {
            startupFinalizing = true;
            clearStartupIntervals();
            showStartupOverlay('TVHeadend is healthy. Finalising first-run setup...');
            setTimeout(saveFirstRunSettings, 1000);
          }
        }
      })
      .catch(() => {
        showStartupOverlay('Waiting for TVHeadend to become healthy for first-run setup...');
      });
  };

  const saveFirstRunSettings = () => {
    let postData = {
      settings: {
        first_run: true,
        app_url: window.location.origin,
      },
    };
    axios({
      method: 'POST',
      url: '/tic-api/save-settings',
      data: postData,
    })
      .then(() => {
        hideStartupOverlay();
        // Reload page to properly trigger the auth refresh
        location.reload();
      })
      .catch(() => {
        startupFinalizing = false;
        showStartupOverlay('Finalising first-run setup failed. Retrying...');
        pingBackend();
      });
  };

  const checkTvhRunning = () => {
    if (!firstRun.value || startupFinalizing) {
      return;
    }
    axios({
      method: 'get',
      url: '/tic-api/tvh-running',
    })
      .then((response) => {
        aioMode.value = response.data.data.running;
        if (response.data.data.running) {
          showStartupOverlay('TVHeadend process detected. Waiting for health check...');
          if (!pingInterval) {
            pingBackend();
            pingInterval = setInterval(pingBackend, 5000);
          }
          if (runningCheckInterval) {
            clearInterval(runningCheckInterval);
            runningCheckInterval = null;
          }
        } else {
          showStartupOverlay('Waiting for TVHeadend process to start for first-run setup...');
        }
      })
      .catch(() => {
        showStartupOverlay('Checking TVHeadend startup status...');
      });
  };

  const checkTvhStatus = () => {
    // Fetch current settings
    axios({
      method: 'get',
      url: '/tic-api/get-settings',
    })
      .then((response) => {
        firstRun.value = response.data.data.first_run;
        firstRunInitComplete.value = true;
        if (firstRun.value) {
          showStartupOverlay('Waiting for TVHeadend to start for first-run setup...');
          checkTvhRunning();
          runningCheckInterval = setInterval(checkTvhRunning, 5000);
        } else {
          hideStartupOverlay();
          // After first-run, detect AIO mode once and never gate the UI on TVH health again.
          axios({
            method: 'get',
            url: '/tic-api/tvh-running',
          })
            .then((tvhResponse) => {
              aioMode.value = tvhResponse.data.data.running;
            })
            .catch(() => {});
        }
      })
      .catch(() => {});
  };

  checkTvhStatus();

  onBeforeUnmount(() => {
    clearStartupIntervals();
    if (firstRunInitComplete.value && !firstRun.value) {
      hideStartupOverlay();
    }
  });

  return {
    firstRun,
    aioMode,
  };
}

export default function getAioStartupTasks() {
  if (!instance) {
    instance = createAioStartupTasks();
  }
  return instance;
}
