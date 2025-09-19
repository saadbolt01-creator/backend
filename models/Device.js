const database = require('../config/database');

class Device {
  constructor(data = {}) {
    this.id = data.id;
    this.company_id = data.company_id;
    this.device_type_id = data.device_type_id;
    this.serial_number = data.serial_number;
    this.metadata = data.metadata;
    this.created_at = data.created_at;
    this.device_type_name = data.device_type_name;
    this.hierarchy_name = data.hierarchy_name;
    this.company_name = data.company_name;
  }

  static async findById(id) {
    const query = `
      SELECT d.*, dt.type_name as device_type_name, c.name as company_name
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      JOIN company c ON d.company_id = c.id
      WHERE d.id = $1
    `;
    const result = await database.query(query, [id]);
    return result.rows[0] ? new Device(result.rows[0]) : null;
  }

  static async findByCompany(company_id) {
    const query = `
      SELECT d.*, dt.type_name as device_type_name, c.name as company_name
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      JOIN company c ON d.company_id = c.id
      WHERE d.company_id = $1
      ORDER BY d.serial_number
    `;
    const result = await database.query(query, [company_id]);
    return result.rows.map(row => new Device(row));
  }

  static async getDeviceChartData(device_id, timeRange = 'day') {
    let timeFilter = '';
    let groupBy = '';
    
    switch (timeRange) {
      case 'hour':
        timeFilter = "dd.created_at >= now() - interval '1 hour'";
        groupBy = "date_trunc('minute', dd.created_at)";
        break;
      case 'day':
        timeFilter = "dd.created_at >= date_trunc('day', now())";
        groupBy = "date_trunc('minute', dd.created_at)";
        break;
      case 'week':
        timeFilter = "dd.created_at >= now() - interval '7 days'";
        groupBy = "date_trunc('hour', dd.created_at)";
        break;
      case 'month':
        timeFilter = "dd.created_at >= now() - interval '30 days'";
        groupBy = "date_trunc('day', dd.created_at)";
        break;
      default:
        timeFilter = "dd.created_at >= date_trunc('day', now())";
        groupBy = "date_trunc('minute', dd.created_at)";
    }

    const query = `
      SELECT 
        ${groupBy} AS time_period,
        AVG((dd.data->>'GFR')::numeric) AS avg_gfr,
        AVG((dd.data->>'GOR')::numeric) AS avg_gor,
        AVG((dd.data->>'GVF')::numeric) AS avg_gvf,
        AVG((dd.data->>'OFR')::numeric) AS avg_ofr,
        AVG((dd.data->>'WFR')::numeric) AS avg_wfr,
        AVG((dd.data->>'WLR')::numeric) AS avg_wlr,
        AVG((dd.data->>'PressureAvg')::numeric) AS avg_pressure,
        AVG((dd.data->>'TemperatureAvg')::numeric) AS avg_temp,
        COUNT(*) as data_points
      FROM device_data dd
      WHERE dd.device_id = $1 AND ${timeFilter}
      GROUP BY ${groupBy}
      ORDER BY time_period
    `;

    const result = await database.query(query, [device_id]);
    return result.rows;
  }

  static async getHierarchyChartData(hierarchy_id, timeRange = 'day') {
    let timeFilter = '';
    let groupBy = '';
    
    switch (timeRange) {
      case 'hour':
        timeFilter = "dd.created_at >= now() - interval '1 hour'";
        groupBy = "date_trunc('minute', dd.created_at)";
        break;
      case 'day':
        timeFilter = "dd.created_at >= date_trunc('day', now())";
        groupBy = "date_trunc('minute', dd.created_at)";
        break;
      case 'week':
        timeFilter = "dd.created_at >= now() - interval '7 days'";
        groupBy = "date_trunc('hour', dd.created_at)";
        break;
      case 'month':
        timeFilter = "dd.created_at >= now() - interval '30 days'";
        groupBy = "date_trunc('day', dd.created_at)";
        break;
      default:
        timeFilter = "dd.created_at >= date_trunc('day', now())";
        groupBy = "date_trunc('minute', dd.created_at)";
    }

    const query = `
      WITH RECURSIVE hierarchy_cte AS (
        -- Start from the selected hierarchy
        SELECT id
        FROM hierarchy
        WHERE id = $1

        UNION ALL

        -- Recursive step: fetch all children
        SELECT h.id
        FROM hierarchy h
        JOIN hierarchy_cte c ON h.parent_id = c.id
      ),
      devices AS (
        -- Devices attached to hierarchies under this tree
        SELECT d.id, d.serial_number, hd.hierarchy_id
        FROM device d
        JOIN hierarchy_device hd ON d.id = hd.device_id
        WHERE hd.hierarchy_id IN (
          SELECT id FROM hierarchy_cte
        )
      ),
      device_data_minute AS (
        -- Average per device per time period
        SELECT 
          dd.device_id,
          ${groupBy} AS minute,
          AVG((dd.data->>'GFR')::numeric) AS avg_gfr,
          AVG((dd.data->>'GOR')::numeric) AS avg_gor,
          AVG((dd.data->>'GVF')::numeric) AS avg_gvf,
          AVG((dd.data->>'OFR')::numeric) AS avg_ofr,
          AVG((dd.data->>'WFR')::numeric) AS avg_wfr,
          AVG((dd.data->>'WLR')::numeric) AS avg_wlr,
          AVG((dd.data->>'PressureAvg')::numeric) AS avg_pressure,
          AVG((dd.data->>'TemperatureAvg')::numeric) AS avg_temp
        FROM device_data dd
        JOIN devices d ON d.id = dd.device_id
        WHERE ${timeFilter}
        GROUP BY dd.device_id, ${groupBy}
      ),
      summed AS (
        -- Sum across devices per time period
        SELECT 
          minute,
          SUM(avg_gfr) AS total_gfr,
          SUM(avg_gor) AS total_gor,
          SUM(avg_ofr) AS total_ofr,
          SUM(avg_wfr) AS total_wfr,
          CASE 
            WHEN COALESCE(SUM(avg_gfr), 0) + COALESCE(SUM(avg_ofr), 0) + COALESCE(SUM(avg_wfr), 0) > 0 
            THEN COALESCE(SUM(avg_gfr), 0) * 100.0 / (COALESCE(SUM(avg_gfr), 0) + COALESCE(SUM(avg_ofr), 0) + COALESCE(SUM(avg_wfr), 0))
            ELSE 0 
          END AS total_gvf,
          CASE 
            WHEN COALESCE(SUM(avg_ofr), 0) + COALESCE(SUM(avg_wfr), 0) > 0 
            THEN COALESCE(SUM(avg_wfr), 0) * 100.0 / (COALESCE(SUM(avg_ofr), 0) + COALESCE(SUM(avg_wfr), 0))
            ELSE 0 
          END AS total_wlr,
          AVG(avg_pressure) AS avg_pressure,
          AVG(avg_temp) AS avg_temp,
          COUNT(DISTINCT device_id) as device_count
        FROM device_data_minute
        GROUP BY minute
      )
      SELECT * 
      FROM summed
      ORDER BY minute
    `;

    const result = await database.query(query, [hierarchy_id]);
    return result.rows;
  }

  static async getLatestDeviceData(device_id) {
    const query = `
      SELECT 
        dd.*,
        d.serial_number,
        dt.type_name as device_type
      FROM device_data dd
      JOIN device d ON dd.device_id = d.id
      JOIN device_type dt ON d.device_type_id = dt.id
      WHERE dd.device_id = $1
      ORDER BY dd.created_at DESC
      LIMIT 1
    `;

    const result = await database.query(query, [device_id]);
    return result.rows[0] || null;
  }

  // Debug method to check what devices are found for a hierarchy
  static async getDevicesForHierarchy(hierarchy_id) {
    const query = `
      WITH RECURSIVE hierarchy_cte AS (
        SELECT id, name, level_id
        FROM hierarchy
        WHERE id = $1

        UNION ALL

        SELECT h.id, h.name, h.level_id
        FROM hierarchy h
        JOIN hierarchy_cte c ON h.parent_id = c.id
      )
      SELECT 
        h.id as hierarchy_id,
        h.name as hierarchy_name,
        hl.name as level_name,
        d.id as device_id,
        d.serial_number,
        dt.type_name,
        COUNT(dd.id) as data_count
      FROM hierarchy_cte h
      JOIN hierarchy_level hl ON h.level_id = hl.id
      LEFT JOIN hierarchy_device hd ON h.id = hd.hierarchy_id
      LEFT JOIN device d ON hd.device_id = d.id
      LEFT JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN device_data dd ON d.id = dd.device_id AND dd.created_at >= date_trunc('day', now())
      GROUP BY h.id, h.name, hl.name, d.id, d.serial_number, dt.type_name
      ORDER BY h.id, d.serial_number
    `;

    const result = await database.query(query, [hierarchy_id]);
    return result.rows;
  }
  toJSON() {
    return {
      id: this.id,
      company_id: this.company_id,
      device_type_id: this.device_type_id,
      serial_number: this.serial_number,
      metadata: this.metadata,
      created_at: this.created_at,
      device_type_name: this.device_type_name,
      hierarchy_name: this.hierarchy_name,
      company_name: this.company_name
    };
  }
}

module.exports = Device;