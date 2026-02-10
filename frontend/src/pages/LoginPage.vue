<template>
  <q-layout view="hHh LpR lFf">
    <q-page-container>
      <q-page class="login-page row items-center justify-center">
        <q-card class="login-card q-pa-lg" style="width: 360px; max-width: 90vw;">
          <q-card-section>
            <div class="text-h6">Sign in</div>
            <div class="text-caption text-grey-7">Use your username and password</div>
          </q-card-section>

          <q-form @submit.prevent="handleLogin">
            <q-card-section>
              <q-input v-model="username" label="Username" autofocus />
              <q-input v-model="password" label="Password" type="password" class="q-mt-md" />
            </q-card-section>

            <q-card-actions align="right">
              <q-btn color="primary" label="Login" :loading="loading" type="submit" />
            </q-card-actions>
          </q-form>
        </q-card>
      </q-page>
    </q-page-container>
  </q-layout>
</template>

<script>
import {ref} from 'vue';
import {useRouter} from 'vue-router';
import {useQuasar} from 'quasar';
import {useAuthStore} from 'stores/auth';

export default {
  setup() {
    const $q = useQuasar();
    const router = useRouter();
    const authStore = useAuthStore();
    const username = ref('');
    const password = ref('');
    const loading = ref(false);

    if (authStore.token) {
      authStore.checkAuthentication().then(() => {
        if (authStore.isAuthenticated) {
          router.push({path: '/'});
        }
      });
    }

    const handleLogin = async () => {
      loading.value = true;
      try {
        const response = await authStore.login(username.value, password.value);
        if (response.status === 200 && response.data.success) {
          await router.push({path: '/'});
        }
      } catch (error) {
        $q.notify({
          color: 'negative',
          position: 'top',
          message: 'Login failed',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      } finally {
        loading.value = false;
      }
    };

    return {
      username,
      password,
      loading,
      handleLogin,
    };
  },
};
</script>
