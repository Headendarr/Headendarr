import {ref, onBeforeUnmount} from 'vue';

export default function pollForBackgroundTasks() {
  const pendingTasks = ref([]);
  const pendingTasksStatus = ref('running');
  let timerId = null;
  let abortController = null;
  let isActive = true;
  let unauthorized = false;

  async function fetchData() {
    if (!isActive) {
      return;
    }
    if (unauthorized) {
      startTimer(10000);
      return;
    }
    if (abortController) {
      abortController.abort();
    }
    abortController = new AbortController();
    try {
      const response = await fetch('/tic-api/get-background-tasks?wait=1&timeout=25', {
        signal: abortController.signal,
        cache: 'no-store',
      });
      if (response.status === 401) {
        unauthorized = true;
        pendingTasks.value = [];
        pendingTasksStatus.value = 'running';
        return;
      }
      if ([502, 504].includes(response.status)) {
        return;
      }
      if (response.ok) {
        let payload = await response.json();
        let tasks = [];
        if (payload.data['current_task']) {
          tasks.push({
            icon: 'pending',
            name: payload.data['current_task'],
            taskState: 'running',
          });
        }
        for (let i in payload.data['pending_tasks']) {
          tasks.push({
            icon: 'radio_button_unchecked',
            name: payload.data['pending_tasks'][i],
            taskState: 'queued',
          });
        }
        pendingTasks.value = tasks;
        pendingTasksStatus.value = payload.data['task_queue_status'];
      }
    } catch (error) {
      if (error?.name !== 'AbortError') {
        console.error('Background task poll failed:', error);
      }
    }
    startTimer();
  }

  function startTimer(delayMs = 1000) {
    timerId = setTimeout(fetchData, delayMs);
  }

  function stopTimer() {
    clearTimeout(timerId);
    if (abortController) {
      abortController.abort();
    }
  }

  fetchData();

  onBeforeUnmount(() => {
    isActive = false;
    stopTimer();
  });

  return {
    pendingTasks,
    pendingTasksStatus,
  };
}
