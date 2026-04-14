import { Link } from 'react-router-dom'

export function Footer() {
  return (
    <footer className="footer">
      <div className="footer-inner">
        <div className="footer-left">
          <img src="/logo.svg" alt="kasoku" className="footer-logo-img" />
          <p className="footer-copy">
            Distributed key-value storage engine. Built with Go.
          </p>
        </div>
        <div className="footer-right">
          <Link to="/dashboard" className="footer-link">
            Dashboard
          </Link>
          <a href="#features" className="footer-link">
            Features
          </a>
          <a href="#architecture" className="footer-link">
            Architecture
          </a>
        </div>
      </div>
    </footer>
  )
}
