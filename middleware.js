import { NextResponse } from 'next/server';

export const config = {
  matcher: ['/', '/index.html'],
};

export function middleware(request) {
  // Check for auth cookie
  const authCookie = request.cookies.get('auth');
  
  // If authenticated, allow access
  if (authCookie?.value === 'authenticated') {
    return NextResponse.next();
  }
  
  // Check if this is a login attempt
  const url = new URL(request.url);
  const password = url.searchParams.get('password');
  
  if (password === 'biome') {
    // Set auth cookie and redirect to clean URL
    const response = NextResponse.redirect(url.origin);
    response.cookies.set('auth', 'authenticated', {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'strict',
      maxAge: 60 * 60 * 24 * 7, // 7 days
      path: '/',
    });
    return response;
  }
  
  // Show login page
  const incorrectPassword = password !== null && password !== 'biome';
  
  return new NextResponse(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Login - Sports Volume Dashboard</title>
      <style>
        * {
          box-sizing: border-box;
          margin: 0;
          padding: 0;
        }
        body {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
          display: flex;
          align-items: center;
          justify-content: center;
          min-height: 100vh;
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }
        .login-container {
          background: white;
          padding: 50px 40px;
          border-radius: 16px;
          box-shadow: 0 20px 60px rgba(0,0,0,0.3);
          text-align: center;
          max-width: 420px;
          width: 90%;
          animation: slideUp 0.4s ease-out;
        }
        @keyframes slideUp {
          from {
            opacity: 0;
            transform: translateY(20px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
        .lock-icon {
          font-size: 48px;
          margin-bottom: 20px;
        }
        h1 {
          color: #333;
          font-size: 24px;
          margin-bottom: 10px;
        }
        .subtitle {
          color: #666;
          margin-bottom: 30px;
          font-size: 14px;
        }
        form {
          display: flex;
          flex-direction: column;
          gap: 15px;
        }
        input[type="password"] {
          padding: 14px 16px;
          border: 2px solid #e0e0e0;
          border-radius: 8px;
          font-size: 16px;
          transition: border-color 0.2s;
        }
        input[type="password"]:focus {
          outline: none;
          border-color: #667eea;
        }
        button {
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          color: white;
          border: none;
          padding: 14px;
          border-radius: 8px;
          font-size: 16px;
          font-weight: 600;
          cursor: pointer;
          transition: transform 0.2s, box-shadow 0.2s;
        }
        button:hover {
          transform: translateY(-2px);
          box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
        }
        button:active {
          transform: translateY(0);
        }
        .error {
          color: #dc3545;
          font-size: 14px;
          padding: 10px;
          background: #f8d7da;
          border-radius: 6px;
          display: ${incorrectPassword ? 'block' : 'none'};
          animation: shake 0.4s;
        }
        @keyframes shake {
          0%, 100% { transform: translateX(0); }
          25% { transform: translateX(-10px); }
          75% { transform: translateX(10px); }
        }
      </style>
    </head>
    <body>
      <div class="login-container">
        <div class="lock-icon">🔒</div>
        <h1>Protected Dashboard</h1>
        <p class="subtitle">Sports Volume Executive Report</p>
        <form method="GET" action="/">
          <input 
            type="password" 
            name="password" 
            placeholder="Enter password" 
            required 
            autofocus
            autocomplete="current-password"
            value=""
          >
          <button type="submit">Access Dashboard</button>
          ${incorrectPassword ? '<div class="error">❌ Incorrect password. Please try again.</div>' : ''}
        </form>
      </div>
    </body>
    </html>
  `, {
    status: 401,
    headers: {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store',
    },
  });
}
