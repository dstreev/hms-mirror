import React, { useState, useRef, useEffect } from 'react';
import { MagnifyingGlassIcon, XMarkIcon } from '@heroicons/react/24/outline';

interface SearchableInputProps<T> {
  value: string | null;
  onSearch: (query: string) => void;
  onSelect: (item: T) => void;
  onClear?: () => void;
  options: T[];
  getOptionLabel: (option: T) => string;
  getOptionKey: (option: T) => string;
  placeholder?: string;
  label?: string;
  disabled?: boolean;
  error?: string;
}

function SearchableInput<T>({
  value,
  onSearch,
  onSelect,
  onClear,
  options,
  getOptionLabel,
  getOptionKey,
  placeholder = "Type to search...",
  label,
  disabled = false,
  error
}: SearchableInputProps<T>) {
  const [isOpen, setIsOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpen(false);
        setSearchQuery('');
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const query = e.target.value;
    setSearchQuery(query);
    setIsOpen(true);
    onSearch(query);
  };

  const handleInputFocus = () => {
    setIsOpen(true);
    if (!searchQuery) {
      onSearch('');
    }
  };

  const handleSelect = (item: T) => {
    onSelect(item);
    setSearchQuery('');
    setIsOpen(false);
    inputRef.current?.blur();
  };

  const handleClear = () => {
    if (onClear) {
      onClear();
      setSearchQuery('');
      setIsOpen(false);
      inputRef.current?.focus();
    }
  };

  return (
    <div className="w-full" ref={containerRef}>
      {label && (
        <label className="block text-sm font-medium text-gray-700 mb-2">
          {label}
        </label>
      )}
      
      <div className="relative">
        {/* Selected Value Display */}
        {value && !isOpen && (
          <div className={`w-full px-3 py-2 border rounded-md bg-gray-50 flex items-center justify-between ${
            error ? 'border-red-500' : 'border-gray-300'
          } ${disabled ? 'cursor-not-allowed opacity-50' : ''}`}>
            <span className="text-gray-900">{value}</span>
            {onClear && !disabled && (
              <button
                type="button"
                onClick={handleClear}
                className="ml-2 text-gray-400 hover:text-gray-600 focus:outline-none"
              >
                <XMarkIcon className="h-4 w-4" />
              </button>
            )}
          </div>
        )}

        {/* Search Input */}
        {(!value || isOpen) && (
          <>
            <div className="relative">
              <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                <MagnifyingGlassIcon className="h-5 w-5 text-gray-400" />
              </div>
              <input
                ref={inputRef}
                type="text"
                value={searchQuery}
                onChange={handleInputChange}
                onFocus={handleInputFocus}
                placeholder={value ? value : placeholder}
                disabled={disabled}
                className={`w-full pl-10 pr-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 ${
                  error ? 'border-red-500' : 'border-gray-300'
                } ${disabled ? 'cursor-not-allowed opacity-50 bg-gray-100' : 'bg-white'}`}
              />
            </div>

            {/* Dropdown */}
            {isOpen && !disabled && (
              <div className="absolute z-50 w-full mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-48 overflow-y-auto">
                {options.length > 0 ? (
                  options.map((option) => (
                    <button
                      key={getOptionKey(option)}
                      type="button"
                      onClick={() => handleSelect(option)}
                      className="w-full text-left px-3 py-2 hover:bg-gray-100 focus:bg-gray-100 focus:outline-none text-sm border-b border-gray-100 last:border-b-0"
                    >
                      {getOptionLabel(option)}
                    </button>
                  ))
                ) : (
                  <div className="px-3 py-2 text-sm text-gray-500">
                    {searchQuery ? `No results found for "${searchQuery}"` : 'No options available'}
                  </div>
                )}
              </div>
            )}
          </>
        )}

        {/* Toggle Search Button (when value is selected) */}
        {value && !isOpen && !disabled && (
          <button
            type="button"
            onClick={() => setIsOpen(true)}
            className="absolute right-2 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-gray-600 focus:outline-none"
          >
            <MagnifyingGlassIcon className="h-4 w-4" />
          </button>
        )}
      </div>

      {/* Error Message */}
      {error && (
        <p className="mt-1 text-sm text-red-600">{error}</p>
      )}
    </div>
  );
}

export default SearchableInput;