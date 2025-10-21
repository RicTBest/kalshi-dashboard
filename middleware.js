import { NextResponse } from 'next/server';

export const config = {
  matcher: [
    '/',
    '/index.html',
  ],
};

export function middleware(request) {
  // Get authorization header
  const authHeader = request.headers.get('authorization');
  
  // Check for Basic Auth credentials
  // Username: admin, Password: biome
  const validAuth = 'Basic ' + Buffer.from('admin:biome').toString('base64');
  
  // If no auth or invalid auth, prompt for credentials
  if (!authHeader || authHeader !== validAuth) {
    return new NextResponse('Authentication required', {
      status: 401,
      headers: {
        'WWW-Authenticate': 'Basic realm="Secure Area"',
      },
    });
  }
  
  // Allow the request to continue
  return NextResponse.next();
}
