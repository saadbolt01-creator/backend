const database = require('./config/database');

async function debugHierarchy() {
  try {
    await database.connect();
    
    console.log('🔍 Debugging Hierarchy Chart Data Issue\n');
    
    // 1. Check hierarchy structure
    console.log('1. Hierarchy Structure:');
    const hierarchyQuery = `
      SELECT 
        h.id,
        h.name,
        h.company_id,
        hl.name as level_name,
        h.parent_id,
        COUNT(DISTINCT hd.device_id) as device_count
      FROM hierarchy h
      JOIN hierarchy_level hl ON h.level_id = hl.id
      LEFT JOIN hierarchy_device hd ON h.id = hd.hierarchy_id
      GROUP BY h.id, h.name, h.company_id, hl.name, h.parent_id, hl.level_order
      ORDER BY h.company_id, hl.level_order, h.name
    `;
    
    const hierarchies = await database.query(hierarchyQuery);
    hierarchies.rows.forEach(row => {
      console.log(`  ${row.id}: ${row.name} (${row.level_name}) - ${row.device_count} devices`);
    });
    
    // 2. Check devices and their data
    console.log('\n2. Devices with Data:');
    const deviceQuery = `
      SELECT 
        d.id,
        d.serial_number,
        dt.type_name,
        h.name as hierarchy_name,
        COUNT(dd.id) as data_count,
        MAX(dd.created_at) as latest_data
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      JOIN hierarchy_device hd ON d.id = hd.device_id
      JOIN hierarchy h ON hd.hierarchy_id = h.id
      LEFT JOIN device_data dd ON d.id = dd.device_id
      GROUP BY d.id, d.serial_number, dt.type_name, h.name
      ORDER BY d.id
    `;
    
    const devices = await database.query(deviceQuery);
    devices.rows.forEach(row => {
      console.log(`  Device ${row.id}: ${row.serial_number} (${row.type_name}) in ${row.hierarchy_name} - ${row.data_count} data points, latest: ${row.latest_data}`);
    });
    
    // 3. Test the recursive query for hierarchy ID 1
    console.log('\n3. Testing Recursive Query for Hierarchy ID 1:');
    const testQuery = `
      WITH RECURSIVE hierarchy_cte AS (
        SELECT id, name
        FROM hierarchy
        WHERE id = 1

        UNION ALL

        SELECT h.id, h.name
        FROM hierarchy h
        JOIN hierarchy_cte c ON h.parent_id = c.id
      )
      SELECT 
        hc.id as hierarchy_id,
        hc.name as hierarchy_name,
        d.id as device_id,
        d.serial_number,
        COUNT(dd.id) as data_points
      FROM hierarchy_cte hc
      LEFT JOIN hierarchy_device hd ON hc.id = hd.hierarchy_id
      LEFT JOIN device d ON hd.device_id = d.id
      LEFT JOIN device_data dd ON d.id = dd.device_id AND dd.created_at >= date_trunc('day', now())
      GROUP BY hc.id, hc.name, d.id, d.serial_number
      ORDER BY hc.id, d.id
    `;
    
    const testResult = await database.query(testQuery);
    testResult.rows.forEach(row => {
      console.log(`  Hierarchy ${row.hierarchy_id} (${row.hierarchy_name}): Device ${row.device_id} (${row.serial_number}) - ${row.data_points} data points today`);
    });
    
    // 4. Check today's data specifically
    console.log('\n4. Today\'s Data Summary:');
    const todayQuery = `
      SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT device_id) as devices_with_data,
        MIN(created_at) as earliest,
        MAX(created_at) as latest
      FROM device_data 
      WHERE created_at >= date_trunc('day', now())
    `;
    
    const todayResult = await database.query(todayQuery);
    console.log(`  Total records today: ${todayResult.rows[0].total_records}`);
    console.log(`  Devices with data: ${todayResult.rows[0].devices_with_data}`);
    console.log(`  Time range: ${todayResult.rows[0].earliest} to ${todayResult.rows[0].latest}`);
    
    // 5. Test the actual chart data query
    console.log('\n5. Testing Chart Data Query for Hierarchy ID 1:');
    const chartQuery = `
      WITH RECURSIVE hierarchy_cte AS (
        SELECT id
        FROM hierarchy
        WHERE id = 1

        UNION ALL

        SELECT h.id
        FROM hierarchy h
        JOIN hierarchy_cte c ON h.parent_id = c.id
      ),
      devices AS (
        SELECT d.id, d.serial_number, hd.hierarchy_id
        FROM device d
        JOIN hierarchy_device hd ON d.id = hd.device_id
        WHERE hd.hierarchy_id IN (
          SELECT id FROM hierarchy_cte
        )
      ),
      device_data_minute AS (
        SELECT 
          dd.device_id,
          date_trunc('minute', dd.created_at) AS minute,
          AVG((dd.data->>'GFR')::numeric) AS avg_gfr,
          AVG((dd.data->>'OFR')::numeric) AS avg_ofr,
          AVG((dd.data->>'WFR')::numeric) AS avg_wfr
        FROM device_data dd
        JOIN devices d ON d.id = dd.device_id
        WHERE dd.created_at >= date_trunc('day', now())
        GROUP BY dd.device_id, date_trunc('minute', dd.created_at)
      ),
      summed AS (
        SELECT 
          minute,
          SUM(avg_gfr) AS total_gfr,
          SUM(avg_ofr) AS total_ofr,
          SUM(avg_wfr) AS total_wfr,
          COUNT(DISTINCT device_id) as device_count
        FROM device_data_minute
        GROUP BY minute
      )
      SELECT 
        COUNT(*) as data_points,
        MIN(minute) as earliest,
        MAX(minute) as latest,
        AVG(device_count) as avg_devices_per_minute
      FROM summed
    `;
    
    const chartResult = await database.query(chartQuery);
    console.log(`  Chart data points: ${chartResult.rows[0].data_points}`);
    console.log(`  Time range: ${chartResult.rows[0].earliest} to ${chartResult.rows[0].latest}`);
    console.log(`  Average devices per minute: ${chartResult.rows[0].avg_devices_per_minute}`);
    
    await database.disconnect();
    
  } catch (error) {
    console.error('Debug error:', error);
    process.exit(1);
  }
}

debugHierarchy();