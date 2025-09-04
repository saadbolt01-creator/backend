import React, { useState, useEffect } from 'react'
import { companiesAPI } from '../services/api'
import toast from 'react-hot-toast'
import { useForm } from 'react-hook-form'
import { 
  Building2, 
  Plus, 
  Edit, 
  Trash2, 
  Globe, 
  Mail, 
  Calendar,
  X,
  Check,
  Save,
  Eye,
  EyeOff
} from 'lucide-react'
import Modal from '../components/Modal'

interface Company {
  _id: string
  name: string
  domains: string[]
  description?: string
  contactEmail?: string
  isActive: boolean
  createdAt: string
  updatedAt: string
}

interface CompanyForm {
  name: string
  domains: string
  description: string
  contactEmail: string
  isActive: boolean
}

export default function Companies() {
  const [companies, setCompanies] = useState<Company[]>([])
  const [loading, setLoading] = useState(true)
  const [showModal, setShowModal] = useState(false)
  const [editingCompany, setEditingCompany] = useState<Company | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [showInactive, setShowInactive] = useState(true)

  const { register, handleSubmit, formState: { errors }, reset, setValue } = useForm<CompanyForm>()

  useEffect(() => {
    loadCompanies()
  }, [])

  const loadCompanies = async () => {
    try {
      const response = await companiesAPI.getAllCompanies()
      if (response.data.success) {
        setCompanies(response.data.data.companies)
      }
    } catch (error: any) {
      toast.error('Failed to load companies')
    } finally {
      setLoading(false)
    }
  }

  const openModal = (company?: Company) => {
    if (company) {
      setEditingCompany(company)
      setValue('name', company.name)
      setValue('domains', company.domains.join(', '))
      setValue('description', company.description || '')
      setValue('contactEmail', company.contactEmail || '')
      setValue('isActive', company.isActive)
    } else {
      setEditingCompany(null)
      reset()
      setValue('isActive', true)
    }
    setShowModal(true)
  }

  const closeModal = () => {
    setShowModal(false)
    setEditingCompany(null)
    reset()
  }

  const onSubmit = async (data: CompanyForm) => {
    setSubmitting(true)
    try {
      const companyData = {
        name: data.name,
        domains: data.domains.split(',').map(d => d.trim()).filter(d => d),
        description: data.description,
        contactEmail: data.contactEmail,
        isActive: data.isActive
      }

      if (editingCompany) {
        await companiesAPI.updateCompany(editingCompany._id, companyData)
        toast.success('Company updated successfully')
      } else {
        await companiesAPI.createCompany(companyData)
        toast.success('Company created successfully')
      }
      
      closeModal()
      loadCompanies()
    } catch (error: any) {
      toast.error(error.response?.data?.message || 'Operation failed')
    } finally {
      setSubmitting(false)
    }
  }

  const deleteCompany = async (companyId: string, companyName: string) => {
    if (!confirm(`Are you sure you want to delete ${companyName}? This will prevent new user registrations from this company.`)) {
      return
    }

    try {
      await companiesAPI.deleteCompany(companyId)
      toast.success('Company deleted successfully')
      loadCompanies()
    } catch (error: any) {
      toast.error('Failed to delete company')
    }
  }

  const filteredCompanies = companies.filter(company => 
    showInactive ? true : company.isActive
  )

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
          <h1 className="text-3xl font-bold text-gray-900">Companies Management</h1>
          <p className="text-gray-600 mt-1">Manage approved companies and their domains</p>
        </div>
        <div className="flex items-center space-x-3">
          <button
            onClick={() => setShowInactive(!showInactive)}
            className="btn-secondary flex items-center"
          >
            {showInactive ? <EyeOff className="w-4 h-4 mr-2" /> : <Eye className="w-4 h-4 mr-2" />}
            {showInactive ? 'Hide Inactive' : 'Show All'}
          </button>
          <button onClick={() => openModal()} className="btn-primary flex items-center">
            <Plus className="w-4 h-4 mr-2" />
            Add Company
          </button>
        </div>
      </div>

      {/* Companies Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {filteredCompanies.map((company) => (
          <div key={company._id} className="card hover:shadow-lg transition-all duration-200 border-l-4 border-l-primary-500">
            <div className="flex items-start justify-between mb-4">
              <div className="flex items-center flex-1">
                <div className="w-14 h-14 bg-gradient-to-br from-purple-400 to-purple-500 rounded-xl flex items-center justify-center shadow-lg">
                  <Building2 className="w-7 h-7 text-white" />
                </div>
                <div className="ml-4 flex-1">
                  <h3 className="text-lg font-bold text-gray-900 mb-1">{company.name}</h3>
                  <div className="flex items-center">
                    {company.isActive ? (
                      <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
                        <Check className="w-3 h-3 mr-1" />
                        Active
                      </span>
                    ) : (
                      <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-red-100 text-red-800">
                        <X className="w-3 h-3 mr-1" />
                        Inactive
                      </span>
                    )}
                  </div>
                </div>
              </div>
              <div className="flex space-x-1">
                <button
                  onClick={() => openModal(company)}
                  className="p-2 text-blue-600 hover:text-blue-800 hover:bg-blue-50 rounded-lg transition-colors"
                  title="Edit company"
                >
                  <Edit className="w-4 h-4" />
                </button>
                <button
                  onClick={() => deleteCompany(company._id, company.name)}
                  className="p-2 text-red-600 hover:text-red-800 hover:bg-red-50 rounded-lg transition-colors"
                  title="Delete company"
                >
                  <Trash2 className="w-4 h-4" />
                </button>
              </div>
            </div>

            {company.description && (
              <p className="text-sm text-gray-600 mb-4 line-clamp-2">{company.description}</p>
            )}

            <div className="space-y-3">
              <div className="flex items-center text-sm text-gray-500">
                <Globe className="w-4 h-4 mr-2 text-blue-500" />
                <span className="font-medium">{company.domains.length} domain{company.domains.length !== 1 ? 's' : ''}</span>
              </div>
              <div className="flex flex-wrap gap-1">
                {company.domains.map((domain, index) => (
                  <span
                    key={index}
                    className="inline-flex items-center px-2.5 py-1 rounded-lg text-xs font-medium bg-blue-50 text-blue-700 border border-blue-200"
                  >
                    {domain}
                  </span>
                ))}
              </div>
              
              {company.contactEmail && (
                <div className="flex items-center text-sm text-gray-500">
                  <Mail className="w-4 h-4 mr-2 text-green-500" />
                  <span className="truncate">{company.contactEmail}</span>
                </div>
              )}
              
              <div className="flex items-center text-sm text-gray-500">
                <Calendar className="w-4 h-4 mr-2 text-purple-500" />
                <span>Created {new Date(company.createdAt).toLocaleDateString()}</span>
              </div>
            </div>
          </div>
        ))}
      </div>

      {filteredCompanies.length === 0 && (
        <div className="text-center py-12">
          <Building2 className="mx-auto h-12 w-12 text-gray-400" />
          <h3 className="mt-2 text-sm font-medium text-gray-900">No companies found</h3>
          <p className="mt-1 text-sm text-gray-500">
            {showInactive ? 'Get started by creating a new company.' : 'No active companies found. Try showing all companies.'}
          </p>
          <button onClick={() => openModal()} className="mt-4 btn-primary">
            <Plus className="w-4 h-4 mr-2" />
            Add Company
          </button>
        </div>
      )}

      {/* Company Modal */}
      <Modal isOpen={showModal} onClose={closeModal} title={editingCompany ? 'Edit Company' : 'Add New Company'}>
        <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Company Name *
            </label>
            <input
              {...register('name', { required: 'Company name is required' })}
              className="input-field"
              placeholder="Enter company name"
            />
            {errors.name && (
              <p className="mt-1 text-sm text-red-600">{errors.name.message}</p>
            )}
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Domains * (comma-separated)
            </label>
            <input
              {...register('domains', { required: 'At least one domain is required' })}
              className="input-field"
              placeholder="example.com, example.org"
            />
            {errors.domains && (
              <p className="mt-1 text-sm text-red-600">{errors.domains.message}</p>
            )}
            <p className="mt-1 text-xs text-gray-500">
              Enter multiple domains separated by commas
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Description
            </label>
            <textarea
              {...register('description')}
              rows={3}
              className="input-field"
              placeholder="Company description (optional)"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Contact Email
            </label>
            <input
              {...register('contactEmail', {
                pattern: {
                  value: /^\S+@\S+$/i,
                  message: 'Invalid email address'
                }
              })}
              type="email"
              className="input-field"
              placeholder="contact@company.com (optional)"
            />
            {errors.contactEmail && (
              <p className="mt-1 text-sm text-red-600">{errors.contactEmail.message}</p>
            )}
          </div>

          <div className="flex items-center">
            <input
              {...register('isActive')}
              type="checkbox"
              className="h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded"
            />
            <label className="ml-2 block text-sm text-gray-900">
              Company Active (allows new user registrations)
            </label>
          </div>

          <div className="flex justify-end space-x-3 pt-4">
            <button
              type="button"
              onClick={closeModal}
              className="btn-secondary"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={submitting}
              className="btn-primary disabled:opacity-50"
            >
              {submitting ? (
                <div className="flex items-center">
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                  {editingCompany ? 'Updating...' : 'Creating...'}
                </div>
              ) : (
                <>
                  <Save className="w-4 h-4 mr-2" />
                  {editingCompany ? 'Update Company' : 'Create Company'}
                </>
              )}
            </button>
          </div>
        </form>
      </Modal>
    </div>
  )
}