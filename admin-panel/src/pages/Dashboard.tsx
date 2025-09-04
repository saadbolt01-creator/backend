import React, { useState, useEffect } from 'react'
import { Users, Building2, UserCheck, AlertTriangle } from 'lucide-react'
import { usersAPI, companiesAPI } from '../services/api'
import toast from 'react-hot-toast'

interface Stats {
  totalUsers: number
  verifiedUsers: number
  totalCompanies: number
  activeCompanies: number
  recentUsers: any[]
  recentCompanies: any[]
}

export default function Dashboard() {
  const [stats, setStats] = useState<Stats>({
    totalUsers: 0,
    verifiedUsers: 0,
    totalCompanies: 0,
    activeCompanies: 0,
    recentUsers: [],
    recentCompanies: []
  })
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    loadDashboardData()
  }, [])

  const loadDashboardData = async () => {
    try {
      const [usersResponse, companiesResponse] = await Promise.all([
        usersAPI.getAllUsers(1, 50),
        companiesAPI.getAllCompanies()
      ])

      if (usersResponse.data.success && companiesResponse.data.success) {
        const users = usersResponse.data.data.users
        const companies = companiesResponse.data.data.companies

        setStats({
          totalUsers: users.length,
          verifiedUsers: users.filter((u: any) => u.isEmailVerified).length,
          totalCompanies: companies.length,
          activeCompanies: companies.filter((c: any) => c.isActive).length,
          recentUsers: users.slice(0, 6),
          recentCompanies: companies.slice(0, 6)
        })
      }
    } catch (error: any) {
      toast.error('Failed to load dashboard data')
    } finally {
      setLoading(false)
    }
  }

  const statCards = [
    {
      title: 'Total Users',
      value: stats.totalUsers,
      icon: Users,
      color: 'bg-blue-500',
      change: `${stats.totalUsers} registered`
    },
    {
      title: 'Verified Users',
      value: stats.verifiedUsers,
      icon: UserCheck,
      color: 'bg-green-500',
      change: `${Math.round((stats.verifiedUsers / stats.totalUsers) * 100) || 0}% verified`
    },
    {
      title: 'Total Companies',
      value: stats.totalCompanies,
      icon: Building2,
      color: 'bg-purple-500',
      change: `${stats.totalCompanies} approved`
    },
    {
      title: 'Active Companies',
      value: stats.activeCompanies,
      icon: AlertTriangle,
      color: 'bg-orange-500',
      change: `${Math.round((stats.activeCompanies / stats.totalCompanies) * 100) || 0}% active`
    }
  ]

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600"></div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Dashboard</h1>
          <p className="text-gray-600 mt-1">Overview of your platform statistics and recent activity</p>
        </div>
        <button
          onClick={loadDashboardData}
          className="btn-secondary flex items-center"
        >
          <Filter className="w-4 h-4 mr-2" />
          Refresh Data
        </button>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {statCards.map((stat) => {
          const Icon = stat.icon
          return (
            <div key={stat.title} className="card hover:shadow-lg transition-all duration-200">
              <div className="flex items-center">
                <div className={`p-4 rounded-xl ${stat.color} shadow-lg`}>
                  <Icon className="w-7 h-7 text-white" />
                </div>
                <div className="ml-5 flex-1">
                  <p className="text-sm font-medium text-gray-600">{stat.title}</p>
                  <p className="text-3xl font-bold text-gray-900 mb-1">{stat.value}</p>
                  <span className="text-xs text-gray-500">{stat.change}</span>
                </div>
              </div>
            </div>
          )
        })}
      </div>

      {/* Recent Activity */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Recent Users */}
        <div className="card">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Recent Users</h3>
          <div className="space-y-3">
            {stats.recentUsers.map((user: any) => (
              <div key={user._id} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                <div className="flex items-center">
                  <div className="w-8 h-8 bg-gradient-to-br from-blue-400 to-blue-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-medium text-xs">
                      {user.firstName[0]}{user.lastName[0]}
                    </span>
                  </div>
                  <div className="ml-3">
                    <p className="text-sm font-medium text-gray-900">
                      {user.firstName} {user.lastName}
                    </p>
                    <p className="text-xs text-gray-500">{user.email}</p>
                  </div>
                </div>
                <div className="flex items-center">
                  {user.isEmailVerified ? (
                    <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
                      Verified
                    </span>
                  ) : (
                    <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
                      Pending
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Recent Companies */}
        <div className="card">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Recent Companies</h3>
          <div className="space-y-3">
            {stats.recentCompanies.map((company: any) => (
              <div key={company._id} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                <div className="flex items-center">
                  <div className="w-8 h-8 bg-gradient-to-br from-purple-400 to-purple-500 rounded-full flex items-center justify-center">
                    <Building2 className="w-4 h-4 text-white" />
                  </div>
                  <div className="ml-3">
                    <p className="text-sm font-medium text-gray-900">{company.name}</p>
                    <p className="text-xs text-gray-500">
                      {company.domains.length} domain{company.domains.length !== 1 ? 's' : ''}
                    </p>
                  </div>
                </div>
                <div className="flex items-center">
                  {company.isActive ? (
                    <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
                      Active
                    </span>
                  ) : (
                    <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-red-100 text-red-800">
                      Inactive
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}