import { createApp, h, ref } from 'vue';

createApp({
  setup() {
    const count = ref(0);
    return () => h('button', { onClick: () => count.value++ }, `Vue count: ${count.value}`);
  },
}).mount('#app');
