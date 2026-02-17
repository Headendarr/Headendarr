import { boot } from 'quasar/wrappers'
import axios from 'axios'
import {useAuthStore} from 'stores/auth'

// Be careful when using SSR for cross-request state pollution
// due to creating a Singleton instance here;
// If any client changes this (global) instance, it might be a
// good idea to move this instance creation inside of the
// "export default () => {}" function below (which runs individually
// for each client)
const api = axios.create({ baseURL: 'https://api.example.com' })

export default boot(({ app, store }) => {
  // for use inside Vue files (Options API) through this.$axios and this.$api

  app.config.globalProperties.$axios = axios
  // ^ ^ ^ this will allow you to use this.$axios (for Vue Options API form)
  //       so you won't necessarily have to import axios in each vue file

  app.config.globalProperties.$api = api
  // ^ ^ ^ this will allow you to use this.$api (for Vue Options API form)
  //       so you can easily perform requests against your app's API

  axios.interceptors.response.use(
    (response) => response,
    async (error) => {
      const originalRequest = error?.config
      const status = error?.response?.status
      if (!originalRequest || status !== 401 || originalRequest.__isRetryRequest) {
        return Promise.reject(error)
      }

      const url = String(originalRequest.url || '')
      if (
        url.includes('/tic-api/auth/login') ||
        url.includes('/tic-api/auth/logout') ||
        url.includes('/tic-api/auth/refresh')
      ) {
        return Promise.reject(error)
      }

      const authStore = useAuthStore(store)
      if (!authStore.token) {
        return Promise.reject(error)
      }

      try {
        await authStore.refreshSession()
        originalRequest.__isRetryRequest = true
        originalRequest.headers = originalRequest.headers || {}
        originalRequest.headers.Authorization = `Bearer ${authStore.token}`
        return axios(originalRequest)
      } catch (refreshError) {
        authStore.clearAuthState()
        return Promise.reject(refreshError)
      }
    },
  )
})

export { api }
