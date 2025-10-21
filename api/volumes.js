export default async function handler(req, res) {
  // CORS headers
  res.setHeader('Access-Control-Allow-Credentials', true);
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With, Accept, Content-Type, Authorization, apikey');

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return;
  }

  const { start_date, end_date } = req.query;

  if (!start_date || !end_date) {
    return res.status(400).json({ 
      error: 'start_date and end_date are required' 
    });
  }

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;

  if (!supabaseUrl || !supabaseKey) {
    console.error('Missing Supabase environment variables');
    return res.status(500).json({ 
      error: 'Server configuration error' 
    });
  }

  try {
    const url = `${supabaseUrl}/rest/v1/daily_volumes?date=gte.${start_date}&date=lte.${end_date}&order=date.asc`;
    
    console.log('Fetching from Supabase:', url);
    
    const response = await fetch(url, {
      headers: {
        'apikey': supabaseKey,
        'Authorization': `Bearer ${supabaseKey}`,
        'Content-Type': 'application/json'
      }
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error('Supabase error:', response.status, errorText);
      throw new Error(`Supabase API error: ${response.status}`);
    }

    const data = await response.json();
    console.log(`Retrieved ${data.length} rows`);

    res.setHeader('Cache-Control', 'public, s-maxage=3600, stale-while-revalidate=86400');
    return res.status(200).json(data);
  } catch (error) {
    console.error('API error:', error);
    return res.status(500).json({ 
      error: error.message || 'Internal server error' 
    });
  }
}
