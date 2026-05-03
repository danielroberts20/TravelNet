import { BrowserRouter, Routes, Route, useLocation } from 'react-router-dom';
import { useEffect } from 'react';
import Layout from './components/Layout';
import Home from './pages/Home';
import About from './pages/About';
import Journey from './pages/Journey';
import Explorer from './pages/Explorer';
import ML from './pages/ML';
import Trevor from './pages/Trevor';

function ScrollToTop() {
  const { pathname } = useLocation();
  useEffect(() => { window.scrollTo(0, 0); }, [pathname]);
  return null;
}

export default function App() {
  return (
    <BrowserRouter>
      <ScrollToTop />
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
