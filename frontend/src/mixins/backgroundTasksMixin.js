import {ref, onBeforeUnmount} from 'vue';
import {Notify} from 'quasar';

export default function pollForBackgroundTasks() {
  const pendingTasks = ref([]);
  const notifications = ref({});
  const pendingTasksStatus = ref('running');
  let timerId = null;
  let abortController = null;
  let isActive = true;

  const displayCurrentTask = (messageId, taskName) => {
    if (!(messageId in notifications.value)) {
      notifications.value[messageId] = Notify.create({
        group: false,
        type: 'ongoing',
        position: 'bottom-left',
        message: `Executing background task: ${taskName}`,
        html: true,
      });
    } else {
      // Update the current status message
      notifications.value[messageId]({
        message: `Executing background task: ${taskName}`,
        html: true,
      });
    }
  };
  const dismissMessages = (messageId) => {
    if (typeof notifications.value === 'undefined') {
      return;
    }
    if (typeof notifications.value[messageId] === 'function') {
      notifications.value[messageId]();
    }
    if (typeof notifications.value[messageId] !== 'undefined') {
      delete notifications.value[messageId];
    }
  };

  async function fetchData() {
    if (!isActive) {
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
      // Check if authentication is required
      if ([401, 502, 504].includes(response.status)) {
        // Stop polling
        return;
      }
      if (response.ok) {
        let payload = await response.json();
        let tasks = [];
        if (payload.data['current_task']) {
          tasks.push({
            'icon': 'pending',
            'name': payload.data['current_task'],
          });
        }
        for (let i in payload.data['pending_tasks']) {
          tasks.push({
            'icon': 'radio_button_unchecked',
            'name': payload.data['pending_tasks'][i],
          });
        }
        pendingTasks.value = tasks;
        pendingTasksStatus.value = payload.data['task_queue_status'];
        if (payload.data['current_task']) {
          displayCurrentTask('currentTask', payload.data['current_task']);
        } else {
          dismissMessages('currentTask');
        }
      }
    } catch (error) {
      if (error?.name !== 'AbortError') {
        console.error('Background task poll failed:', error);
      }
    }
    startTimer();
  }

  function startTimer() {
    timerId = setTimeout(fetchData, 250);
  }

  function stopTimer() {
    clearTimeout(timerId);
    dismissMessages('currentTask');
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
