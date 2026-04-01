import FogOfWarMap from '../components/FogOfWarMap';

// .nav in main.css is position:fixed; height:52px
const NAV_H = 52;

export default function Journey() {
  return (
    <div style={{
      marginTop: NAV_H,
      height: `calc(100dvh - ${NAV_H}px)`,
      position: 'relative',
      overflow: 'hidden',
    }}>
      <FogOfWarMap />
    </div>
  );
}
