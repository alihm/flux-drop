import React from 'react';
import { createRoot } from 'react-dom/client';

function Counter() {
  const [count, setCount] = React.useState(0);
  return React.createElement('button', { onClick: () => setCount(count + 1) }, `React count: ${count}`);
}
createRoot(document.getElementById('app')).render(React.createElement(Counter));
