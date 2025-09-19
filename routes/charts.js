const express = require('express');
const Device = require('../models/Device');
const Hierarchy = require('../models/Hierarchy');
const { protect } = require('../middleware/auth');

const router = express.Router();

// @desc    Get chart data for a specific device
// @route   GET /api/charts/device/:deviceId
// @access  Private
router.get('/device/:deviceId', protect, async (req, res) => {
  try {
    const deviceId = parseInt(req.params.deviceId);
    const timeRange = req.query.timeRange || 'day'; // hour, day, week, month

    // Check if device exists and user has access
    const device = await Device.findById(deviceId);
    if (!device) {
      return res.status(404).json({
        success: false,
        message: 'Device not found'
      });
    }

    // Check if user has access to this device (same company or admin)
    if (req.user.role !== 'admin' && device.company_id !== req.user.company_id) {
      return res.status(403).json({
        success: false,
        message: 'Access denied to this device'
      });
    }

    // Get chart data
    const chartData = await Device.getDeviceChartData(deviceId, timeRange);
    const latestData = await Device.getLatestDeviceData(deviceId);

    res.json({
      success: true,
      message: 'Device chart data retrieved successfully',
      data: {
        device: device.toJSON(),
        chartData: chartData.map(row => ({
          timestamp: row.time_period,
          gfr: parseFloat(row.avg_gfr) || 0,
          gor: parseFloat(row.avg_gor) || 0,
          gvf: parseFloat(row.avg_gvf) || 0,
          ofr: parseFloat(row.avg_ofr) || 0,
          wfr: parseFloat(row.avg_wfr) || 0,
          wlr: parseFloat(row.avg_wlr) || 0,
          pressure: parseFloat(row.avg_pressure) || 0,
          temperature: parseFloat(row.avg_temp) || 0,
          dataPoints: parseInt(row.data_points) || 0
        })),
        latestData: latestData ? {
          timestamp: latestData.created_at,
          data: latestData.data,
          longitude: latestData.longitude,
          latitude: latestData.latitude
        } : null,
        timeRange,
        totalDataPoints: chartData.length
      }
    });
  } catch (error) {
    console.error('Get device chart data error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting device chart data'
    });
  }
});

// @desc    Get aggregated chart data for a hierarchy (region, area, field, well)
// @route   GET /api/charts/hierarchy/:hierarchyId
// @access  Private
router.get('/hierarchy/:hierarchyId', protect, async (req, res) => {
  try {
    const hierarchyId = parseInt(req.params.hierarchyId);
    const timeRange = req.query.timeRange || 'day'; // hour, day, week, month

    // Check if hierarchy exists and user has access
    const hierarchy = await Hierarchy.findById(hierarchyId);
    if (!hierarchy) {
      return res.status(404).json({
        success: false,
        message: 'Hierarchy not found'
      });
    }

    // Check if user has access to this hierarchy (same company or admin)
    if (req.user.role !== 'admin' && hierarchy.company_id !== req.user.company_id) {
      return res.status(403).json({
        success: false,
        message: 'Access denied to this hierarchy'
      });
    }

    // Get aggregated chart data for this hierarchy and all its children
    const chartData = await Device.getHierarchyChartData(hierarchyId, timeRange);

    // Get devices under this hierarchy
    const database = require('../config/database');
    const devicesQuery = `
      WITH RECURSIVE hierarchy_cte AS (
        SELECT id FROM hierarchy WHERE id = $1
        UNION ALL
        SELECT h.id FROM hierarchy h JOIN hierarchy_cte c ON h.parent_id = c.id
      )
      SELECT d.*, dt.type_name as device_type_name, h.name as hierarchy_name,
             dl.data as latest_data, dl.updated_at as latest_data_time
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      JOIN hierarchy_device hd ON d.id = hd.device_id
      JOIN hierarchy h ON hd.hierarchy_id = h.id
      LEFT JOIN device_latest dl ON d.id = dl.device_id
      WHERE hd.hierarchy_id IN (SELECT id FROM hierarchy_cte)
      ORDER BY d.serial_number
    `;
    
    const devicesResult = await database.query(devicesQuery, [hierarchyId]);
    const devices = devicesResult.rows;

    res.json({
      success: true,
      message: 'Hierarchy chart data retrieved successfully',
      data: {
        hierarchy: hierarchy.toJSON(),
        chartData: chartData.map(row => ({
          timestamp: row.minute || row.time_period,
          totalGfr: parseFloat(row.total_gfr) || 0,
          totalGor: parseFloat(row.total_gor) || 0,
          totalOfr: parseFloat(row.total_ofr) || 0,
          totalWfr: parseFloat(row.total_wfr) || 0,
          totalGvf: parseFloat(row.total_gvf) || 0,
          totalWlr: parseFloat(row.total_wlr) || 0,
          avgPressure: parseFloat(row.avg_pressure) || 0,
          avgTemperature: parseFloat(row.avg_temp) || 0,
          deviceCount: parseInt(row.device_count) || 0
        })),
        devices: devices.map(device => ({
          id: device.id,
          serialNumber: device.serial_number,
          deviceType: device.device_type_name,
          hierarchyName: device.hierarchy_name,
          metadata: device.metadata,
          latestData: device.latest_data,
          latestDataTime: device.latest_data_time
        })),
        timeRange,
        totalDataPoints: chartData.length,
        totalDevices: devices.length
      }
    });
  } catch (error) {
    console.error('Get hierarchy chart data error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting hierarchy chart data'
    });
  }
});

// @desc    Get real-time data for a device
// @route   GET /api/charts/device/:deviceId/realtime
// @access  Private
router.get('/device/:deviceId/realtime', protect, async (req, res) => {
  try {
    const deviceId = parseInt(req.params.deviceId);

    // Check if device exists and user has access
    const device = await Device.findById(deviceId);
    if (!device) {
      return res.status(404).json({
        success: false,
        message: 'Device not found'
      });
    }

    // Check if user has access to this device
    if (req.user.role !== 'admin' && device.company_id !== req.user.company_id) {
      return res.status(403).json({
        success: false,
        message: 'Access denied to this device'
      });
    }

    // Get latest data from device_latest table
    const database = require('../config/database');
    const query = `
      SELECT 
        dl.*,
        d.serial_number,
        dt.type_name as device_type
      FROM device_latest dl
      JOIN device d ON dl.device_id = d.id
      JOIN device_type dt ON d.device_type_id = dt.id
      WHERE dl.device_id = $1
    `;

    const result = await database.query(query, [deviceId]);
    const latestData = result.rows[0];

    if (!latestData) {
      // Fallback to latest device_data entry
      const fallbackData = await Device.getLatestDeviceData(deviceId);
      
      return res.json({
        success: true,
        message: 'Real-time device data retrieved successfully',
        data: {
          device: device.toJSON(),
          latestData: fallbackData ? {
            timestamp: fallbackData.created_at,
            receivedAt: fallbackData.created_at,
            data: fallbackData.data,
            longitude: fallbackData.longitude,
            latitude: fallbackData.latitude,
            serialNumber: fallbackData.serial_number,
            deviceType: fallbackData.device_type
          } : null
        }
      });
    }

    res.json({
      success: true,
      message: 'Real-time device data retrieved successfully',
      data: {
        device: device.toJSON(),
        latestData: {
          timestamp: latestData.updated_at,
          receivedAt: latestData.received_at,
          data: latestData.data,
          longitude: latestData.longitude,
          latitude: latestData.latitude,
          serialNumber: latestData.serial_number,
          deviceType: latestData.device_type
        }
      }
    });
  } catch (error) {
    console.error('Get real-time device data error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting real-time device data'
    });
  }
});

// @desc    Get dashboard summary for user's company
// @route   GET /api/charts/dashboard
// @access  Private
router.get('/dashboard', protect, async (req, res) => {
  try {
    let company_id = req.user.company_id;
    
    // If admin requests specific company dashboard
    if (req.query.company_id && req.user.role === 'admin') {
      company_id = parseInt(req.query.company_id);
    }

    const database = require('../config/database');
    
    // Get company info
    const companyResult = await database.query('SELECT name FROM company WHERE id = $1', [company_id]);
    const companyName = companyResult.rows[0]?.name || 'Unknown Company';

    // Get hierarchy statistics
    const hierarchyStatsQuery = `
      SELECT 
        hl.name as level_name,
        COUNT(h.id) as count
      FROM hierarchy h
      JOIN hierarchy_level hl ON h.level_id = hl.id
      WHERE h.company_id = $1
      GROUP BY hl.name, hl.level_order
      ORDER BY hl.level_order
    `;
    
    const hierarchyStatsResult = await database.query(hierarchyStatsQuery, [company_id]);
    
    // Get device statistics
    const deviceStatsQuery = `
      SELECT 
        dt.type_name,
        COUNT(d.id) as count
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      WHERE d.company_id = $1
      GROUP BY dt.type_name
      ORDER BY count DESC
    `;
    
    const deviceStatsResult = await database.query(deviceStatsQuery, [company_id]);

    // Get recent data activity
    const recentActivityQuery = `
      SELECT 
        d.serial_number,
        dt.type_name,
        dd.created_at,
        dd.data
      FROM device_data dd
      JOIN device d ON dd.device_id = d.id
      JOIN device_type dt ON d.device_type_id = dt.id
      WHERE d.company_id = $1
      ORDER BY dd.created_at DESC
      LIMIT 10
    `;
    
    const recentActivityResult = await database.query(recentActivityQuery, [company_id]);

    // Get total counts
    const totalDevicesResult = await database.query('SELECT COUNT(*) as count FROM device WHERE company_id = $1', [company_id]);
    const totalHierarchiesResult = await database.query('SELECT COUNT(*) as count FROM hierarchy WHERE company_id = $1', [company_id]);

    res.json({
      success: true,
      message: 'Dashboard data retrieved successfully',
      data: {
        company: {
          id: company_id,
          name: companyName
        },
        statistics: {
          totalDevices: parseInt(totalDevicesResult.rows[0].count),
          totalHierarchies: parseInt(totalHierarchiesResult.rows[0].count),
          hierarchyBreakdown: hierarchyStatsResult.rows.map(row => ({
            level: row.level_name,
            count: parseInt(row.count)
          })),
          deviceBreakdown: deviceStatsResult.rows.map(row => ({
            type: row.type_name,
            count: parseInt(row.count)
          }))
        },
        recentActivity: recentActivityResult.rows.map(row => ({
          deviceSerial: row.serial_number,
          deviceType: row.type_name,
          timestamp: row.created_at,
          data: row.data
        })),
        userRole: req.user.role
      }
    });
  } catch (error) {
    console.error('Get dashboard data error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting dashboard data'
    });
  }
});

// @desc    Debug endpoint to check devices for hierarchy
// @route   GET /api/charts/debug/hierarchy/:hierarchyId
// @access  Private
router.get('/debug/hierarchy/:hierarchyId', protect, async (req, res) => {
  try {
    const hierarchyId = parseInt(req.params.hierarchyId);

    // Check if hierarchy exists and user has access
    const hierarchy = await Hierarchy.findById(hierarchyId);
    if (!hierarchy) {
      return res.status(404).json({
        success: false,
        message: 'Hierarchy not found'
      });
    }

    // Check if user has access to this hierarchy
    if (req.user.role !== 'admin' && hierarchy.company_id !== req.user.company_id) {
      return res.status(403).json({
        success: false,
        message: 'Access denied to this hierarchy'
      });
    }

    // Get debug information
    const devices = await Device.getDevicesForHierarchy(hierarchyId);
    
    // Get sample chart data
    const chartData = await Device.getHierarchyChartData(hierarchyId, 'day');

    res.json({
      success: true,
      message: 'Debug information retrieved successfully',
      data: {
        hierarchy: hierarchy.toJSON(),
        devices: devices,
        chartDataPoints: chartData.length,
        sampleChartData: chartData.slice(0, 5) // First 5 data points
      }
    });
  } catch (error) {
    console.error('Debug hierarchy error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting debug information'
    });
  }
});

module.exports = router;