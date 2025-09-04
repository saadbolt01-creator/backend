import axios from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000/api'

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Auth API
export const authAPI = {
  setToken: (token: string) => {
    if (token) {
      api.defaults.headers.common['Authorization'] = `Bearer ${token}`
    } else {
      delete api.defaults.headers.common['Authorization']
    }
  },
  
  login: (email: string, password: string) =>
    api.post('/auth/login', { email, password }),
  
  getCurrentUser: () =>
    api.get('/auth/me'),
  
  logout: () =>
    api.post('/auth/logout'),
}

// Users API
export const usersAPI = {
  getAllUsers: (page = 1, limit = 10) =>
    api.get(`/user/all?page=${page}&limit=${limit}`),
  
  updateUser: (id: string, data: any) =>
    api.put(`/user/${id}`, data),
  
  deleteUser: (id: string) =>
    api.delete(`/user/${id}`),
}

// Companies API
export const companiesAPI = {
  getAllCompanies: () =>
    api.get('/company'),
  
  getCompanyById: (id: string) =>
    api.get(`/company/${id}`),
  
  createCompany: (data: any) =>
    api.post('/company', data),
  
  updateCompany: (id: string, data: any) =>
    api.put(`/company/${id}`, data),
  
  deleteCompany: (id: string) =>
    api.delete(`/company/${id}`),
  
  checkDomain: (domain: string) =>
    api.get(`/company/check-domain/${domain}`),
}

export default api