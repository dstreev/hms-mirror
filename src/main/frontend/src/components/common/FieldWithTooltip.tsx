import React, { useState } from 'react';

interface FieldWithTooltipProps {
  label: string;
  tooltip?: string;
  required?: boolean;
  htmlFor?: string;
  className?: string;
}

/**
 * Reusable field label component with hover tooltip
 * Displays an info icon next to the label that shows tooltip text on hover
 */
const FieldWithTooltip: React.FC<FieldWithTooltipProps> = ({
  label,
  tooltip,
  required = false,
  htmlFor,
  className = ''
}) => {
  const [showTooltip, setShowTooltip] = useState(false);

  return (
    <div className={`flex items-center gap-2 ${className}`}>
      <label
        htmlFor={htmlFor}
        className="block text-sm font-medium text-gray-700"
      >
        {label}
        {required && <span className="text-red-500 ml-1">*</span>}
      </label>

      {tooltip && (
        <div className="relative inline-block">
          <div
            onMouseEnter={() => setShowTooltip(true)}
            onMouseLeave={() => setShowTooltip(false)}
            className="cursor-help"
          >
            {/* Info icon */}
            <svg
              className="w-4 h-4 text-gray-400 hover:text-gray-600"
              fill="currentColor"
              viewBox="0 0 20 20"
              xmlns="http://www.w3.org/2000/svg"
            >
              <path
                fillRule="evenodd"
                d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z"
                clipRule="evenodd"
              />
            </svg>
          </div>

          {/* Tooltip popup */}
          {showTooltip && (
            <div
              className="absolute z-50 left-0 top-6 w-80 p-3 text-sm text-gray-700 bg-white border border-gray-300 rounded-lg shadow-lg"
              style={{ whiteSpace: 'normal' }}
            >
              {tooltip}
              {/* Arrow pointing up */}
              <div
                className="absolute -top-2 left-4 w-4 h-4 bg-white border-l border-t border-gray-300 transform rotate-45"
              />
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default FieldWithTooltip;
