<template>
  <q-layout view="hHh LpR lFf">
    <q-page-container>
      <q-page class="login-page row items-center justify-center">
        <q-card class="login-card q-pa-lg" style="width: 360px; max-width: 90vw;">
          <q-card-section>
            <div class="text-h6">Sign in</div>
            <div class="text-caption text-grey-7">
              {{ loginHint }}
            </div>
          </q-card-section>

          <q-form v-if="localLoginEnabled" @submit.prevent="handleLogin">
            <q-card-section>
              <q-input v-model="username" label="Username" autofocus />
              <q-input v-model="password" label="Password" type="password" class="q-mt-md" />
            </q-card-section>

            <q-card-actions align="right">
              <q-btn color="primary" label="Login" :loading="loading" type="submit" />
            </q-card-actions>
          </q-form>

          <q-card-actions align="right" :class="localLoginEnabled ? 'q-pt-none q-px-md q-pb-md' : 'q-pa-md'">
            <q-btn
              v-if="oidcEnabled"
              color="secondary"
              :label="oidcButtonLabel"
              :loading="oidcLoading"
              @click="handleOidcLogin"
            />
          </q-card-actions>

          <q-card-actions v-if="!localLoginEnabled && !oidcEnabled" align="left">
            <div class="text-negative text-caption">No login method is currently enabled.</div>
          </q-card-actions>
          <q-card-actions v-if="oidcError" align="left">
            <div class="text-negative text-caption">{{ oidcError }}</div>
          </q-card-actions>
          <q-card-actions v-if="oidcEnabled && callbackPending" align="left">
            <div class="text-grey-7 text-caption">Completing SSO sign-in...</div>
          </q-card-actions>
        </q-card>
      </q-page>
    </q-page-container>
  </q-layout>
</template>

<script>
import {computed, ref} from 'vue';
import {useRoute, useRouter} from 'vue-router';
import {useQuasar} from 'quasar';
import {useAuthStore} from 'stores/auth';
import {useSettingsStore} from 'stores/settings';
import axios from 'axios';

export default {
  setup() {
    const $q = useQuasar();
    const route = useRoute();
    const router = useRouter();
    const authStore = useAuthStore();
    const settingsStore = useSettingsStore();
    const username = ref('');
    const password = ref('');
    const loading = ref(false);
    const oidcLoading = ref(false);
    const callbackPending = ref(false);
    const oidcError = ref('');
    const defaultStartPage = '/dashboard';
    const startPageKey = 'tic_ui_start_page';
    const isStreamerOnly = (roles = []) => roles.includes('streamer') && !roles.includes('admin');

    const oidcEnabled = computed(() => !!authStore.authOptions?.oidc?.enabled);
    const oidcButtonLabel = computed(() => authStore.authOptions?.oidc?.button_label || 'Sign in with SSO');
    const localLoginEnabled = computed(() => authStore.authOptions?.local_login_enabled !== false);
    const loginHint = computed(() => {
      if (oidcEnabled.value && !localLoginEnabled.value) {
        return 'Use your single sign-on provider';
      }
      if (oidcEnabled.value && localLoginEnabled.value) {
        return 'Use your username and password, or sign in with SSO';
      }
      return 'Use your username and password';
    });

    const resolveAdminStartPage = async () => {
      try {
        const settings = await settingsStore.refreshSettings({force: true});
        const startPage = settings?.ui_settings?.start_page;
        if (startPage) {
          localStorage.setItem(startPageKey, startPage);
          return startPage;
        }
      } catch (error) {
        // Ignore and fall back to last-known value or default.
      }
      return localStorage.getItem(startPageKey) || defaultStartPage;
    };

    const resolvePostLoginRoute = async (roles = []) => {
      if (isStreamerOnly(roles)) {
        return '/guide';
      }
      if (roles.includes('admin')) {
        return await resolveAdminStartPage();
      }
      return '/login';
    };

    const finishOidcCallbackLogin = async () => {
      callbackPending.value = true;
      oidcError.value = '';
      try {
        await authStore.refreshSession({allowCookie: true});
        const roles = authStore.user?.roles || [];
        const startPage = await resolvePostLoginRoute(roles);
        await router.replace({path: startPage});
      } catch (error) {
        console.error('OIDC callback completion failed:', error);
        oidcError.value = 'SSO login failed. Please try again.';
      } finally {
        callbackPending.value = false;
      }
    };

    const handleOidcLogin = async () => {
      oidcLoading.value = true;
      try {
        authStore.startOidcLogin();
      } finally {
        oidcLoading.value = false;
      }
    };

    const loadAuthOptions = async () => {
      await authStore.fetchAuthOptions();
    };

    loadAuthOptions().then(async () => {
      const oidcErrorQuery = String(route.query?.oidc_error || '').trim();
      const oidcSuccess = String(route.query?.oidc || '').trim() === 'success';
      if (oidcErrorQuery) {
        oidcError.value = oidcErrorQuery;
      }
      if (oidcSuccess) {
        await finishOidcCallbackLogin();
        return;
      }

      if (authStore.token) {
        await authStore.checkAuthentication();
      }
      if (authStore.isAuthenticated) {
        const startPage = await resolvePostLoginRoute(authStore.user?.roles || []);
        await router.replace({path: startPage});
      }
    });

    const handleLogin = async () => {
      loading.value = true;
      try {
        const response = await authStore.login(username.value, password.value);
        if (response.status === 200 && response.data.success) {
          const roles = response.data?.user?.roles || [];
          const startPage = await resolvePostLoginRoute(roles);
          await router.replace({path: startPage});
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
      oidcLoading,
      oidcEnabled,
      oidcButtonLabel,
      localLoginEnabled,
      callbackPending,
      oidcError,
      loginHint,
      handleLogin,
      handleOidcLogin,
    };
  },
};
</script>
