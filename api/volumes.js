import { createClient } from '@supabase/supabase-js';

// Use Node.js runtime instead of Edge
export const config = {
  runtime: 'nodejs',
};

export default async function handler(req, res) {
  // CORS headers
  res.setHeader('Access-Control-Allow-Credentials', true);
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With, Accept, Content-Type, Authorization, apikey');

  // Handle OPTIONS for CORS preflight
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

  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_ANON_KEY
  );

  try {
    const { data, error } = await supabase
      .from('daily_volumes')
      .select('*')
      .gte('date', start_date)
      .lte('date', end_date)
      .order('date', { ascending: true });

    if (error) throw error;

    // Cache for 1 hour
    res.setHeader('Cache-Control', 'public, s-maxage=3600, stale-while-revalidate=86400');
    
    return res.status(200).json(data);
  } catch (error) {
    console.error('Supabase error:', error);
    return res.status(500).json({ 
      error: error.message || 'Internal server error' 
    });
  }
}
