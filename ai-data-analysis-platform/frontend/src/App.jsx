import React, { useState } from 'react';

function App() {
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [analysisResult, setAnalysisResult] = useState(null);

  // Temporary view while you build your pages
  if (!isLoggedIn) {
    return (
      <div style={{ padding: '20px', textAlign: 'center' }}>
        <h1>Welcome to AI Data Platform</h1>
        <button onClick={() => setIsLoggedIn(true)}>Login / Start</button>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', height: '100vh' }}>
      <aside style={{ width: '200px', borderRight: '1px solid #ccc' }}>
        <p>Sidebar Content</p>
      </aside>
      <main style={{ flex: 1, padding: '20px' }}>
        <h1>Dashboard Workspace</h1>
        <p>Analysis Result: {analysisResult || "No data yet"}</p>
        <button onClick={() => setIsLoggedIn(false)}>Logout</button>
      </main>
    </div>
  );
}

export default App;