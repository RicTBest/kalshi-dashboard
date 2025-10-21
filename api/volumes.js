export default async function handler(req, res) {
  // Handle CORS
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, apikey');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  // Log for debugging
  console.log('API called with query:', req.query);

  const { start_date, end_date } = req.query;

  if (!start_date || !end_date) {
    return res.status(400).json({ 
      error: 'start_date and end_date query parameters are required' 
    });
  }

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;

  console.log('Environment check:', {
    hasUrl: !!supabaseUrl,
    hasKey: !!supabaseKey,
    urlPreview: supabaseUrl?.substring(0, 30) + '...'
  });

  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ 
      error: 'Server configuration error - missing Supabase credentials',
      details: {
        hasUrl: !!supabaseUrl,
        hasKey: !!supabaseKey
      }
    });
  }

  try {
    const url = `${supabaseUrl}/rest/v1/daily_volumes?date=gte.${start_date}&date=lte.${end_date}&order=date.asc`;
    
    console.log('Fetching from Supabase...');
    
    const response = await fetch(url, {
      headers: {
        'apikey': supabaseKey,
        'Authorization': `Bearer ${supabaseKey}`,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });

    console.log('Supabase response status:', response.status);

    if (!response.ok) {
      const errorText = await response.text();
      console.error('Supabase error:', errorText);
      return res.status(response.status).json({ 
        error: `Supabase error: ${response.status}`,
        details: errorText
      });
    }

    const data = await response.json();
    console.log(`Successfully retrieved ${data.length} rows`);

    res.setHeader('Cache-Control', 'public, s-maxage=3600');
    return res.status(200).json(data);

  } catch (error) {
    console.error('API error:', error);
    return res.status(500).json({ 
      error: 'Internal server error',
      message: error.message,
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
}
