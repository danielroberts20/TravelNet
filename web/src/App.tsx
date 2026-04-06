import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Layout from './components/Layout';
import Home from './pages/Home';
import About from './pages/About';
import Journey from './pages/Journey';
import Explorer from './pages/Explorer';
import ML from './pages/ML';
import Trevor from './pages/Trevor';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<Home />} />
          <Route path="/about" element={<About />} />
          <Route path="/journey" element={<Journey />} />
          <Route path="/explorer" element={<Explorer />} />
          <Route path="/ml" element={<ML />} />
          <Route path="/trevor" element={<Trevor />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
