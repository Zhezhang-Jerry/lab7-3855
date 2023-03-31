import { useState } from 'react';
import './App.css';
import Stats from './components/Stats';

const App = () => {
  const [statuses, setStatuses] = useState({});

  const handleCheckHealthClick = async () => {
    try {
      const res = await fetch('http://34.221.95.160/health/check');
      const data = await res.json();
      setStatuses(data);
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div className="App">
      <h1>Dashboard</h1>
      <button onClick={handleCheckHealthClick}>Check Health</button>
      <Stats statuses={statuses} />
    </div>
  );
};

export default App;
