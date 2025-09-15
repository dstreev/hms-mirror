import React from 'react';
import { motion } from 'framer-motion';
import clsx from 'clsx';

interface BaseCardProps {
  title: string;
  description?: string;
  icon?: React.ReactNode;
  children?: React.ReactNode;
  className?: string;
  expandable?: boolean;
  expanded?: boolean;
  onExpand?: () => void;
  onClick?: () => void;
  status?: 'idle' | 'loading' | 'success' | 'error' | 'warning';
  badge?: string | number;
  disabled?: boolean;
  size?: 'small' | 'medium' | 'large';
}

export const BaseCard: React.FC<BaseCardProps> = ({
  title,
  description,
  icon,
  children,
  className,
  expandable = false,
  expanded = false,
  onExpand,
  onClick,
  status = 'idle',
  badge,
  disabled = false,
  size = 'medium'
}) => {
  const cardVariants = {
    idle: { scale: 1, boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)' },
    hover: { scale: 1.02, boxShadow: '0 10px 15px -3px rgba(0, 0, 0, 0.1)' },
    tap: { scale: 0.98 },
    expanded: { scale: 1, boxShadow: '0 20px 25px -5px rgba(0, 0, 0, 0.1)' }
  };

  const statusColors = {
    idle: 'border-gray-200 bg-white',
    loading: 'border-blue-200 bg-blue-50',
    success: 'border-green-200 bg-green-50',
    error: 'border-red-200 bg-red-50',
    warning: 'border-yellow-200 bg-yellow-50'
  };

  const sizeClasses = {
    small: 'p-4',
    medium: 'p-6',
    large: 'p-8'
  };

  return (
    <motion.div
      className={clsx(
        'relative rounded-lg border cursor-pointer transition-all duration-200',
        statusColors[status],
        sizeClasses[size],
        disabled && 'opacity-50 cursor-not-allowed',
        expanded && 'col-span-full row-span-2',
        className
      )}
      variants={cardVariants}
      initial="idle"
      whileHover={!disabled ? "hover" : "idle"}
      whileTap={!disabled ? "tap" : "idle"}
      animate={expanded ? "expanded" : "idle"}
      onClick={() => {
        if (!disabled) {
          if (expandable && onExpand) {
            onExpand();
          } else if (onClick) {
            onClick();
          }
        }
      }}
    >
      {/* Badge */}
      {badge && (
        <div className="absolute -top-2 -right-2 bg-blue-500 text-white text-xs rounded-full h-6 w-6 flex items-center justify-center">
          {badge}
        </div>
      )}

      {/* Header */}
      <div className="flex items-center space-x-3 mb-4">
        {icon && (
          <div className={clsx(
            'flex-shrink-0 p-2 rounded-lg',
            status === 'success' && 'bg-green-100 text-green-600',
            status === 'error' && 'bg-red-100 text-red-600',
            status === 'warning' && 'bg-yellow-100 text-yellow-600',
            status === 'loading' && 'bg-blue-100 text-blue-600',
            status === 'idle' && 'bg-gray-100 text-gray-600'
          )}>
            {icon}
          </div>
        )}
        
        <div className="flex-1">
          <h3 className="text-lg font-semibold text-gray-900">{title}</h3>
          {description && (
            <p className="text-sm text-gray-500 mt-1">{description}</p>
          )}
        </div>

        {expandable && (
          <motion.div
            animate={{ rotate: expanded ? 180 : 0 }}
            transition={{ duration: 0.2 }}
          >
            <svg className="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </motion.div>
        )}
      </div>

      {/* Content */}
      {children && (
        <motion.div
          initial={false}
          animate={{
            height: expanded ? 'auto' : children ? 'auto' : 0,
            opacity: expanded ? 1 : children ? 1 : 0
          }}
          transition={{ duration: 0.3 }}
          className="overflow-hidden"
        >
          {children}
        </motion.div>
      )}

      {/* Status Indicator */}
      {status === 'loading' && (
        <div className="absolute inset-0 flex items-center justify-center bg-white bg-opacity-75">
          <div className="loading-spinner h-8 w-8 border-blue-500"></div>
        </div>
      )}
    </motion.div>
  );
};

export default BaseCard;