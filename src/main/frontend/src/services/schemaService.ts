/**
 * Service for fetching and caching OpenAPI schema metadata
 * Used to provide field descriptions for tooltips in wizards
 */

interface SchemaProperty {
  description?: string;
  type?: string;
  example?: any;
  default?: any;
  required?: boolean;
}

interface OpenAPISchema {
  components?: {
    schemas?: {
      [key: string]: {
        properties?: {
          [key: string]: SchemaProperty;
        };
      };
    };
  };
}

class SchemaService {
  private schema: OpenAPISchema | null = null;
  private loading: Promise<OpenAPISchema> | null = null;

  /**
   * Fetch the OpenAPI schema from the backend
   */
  private async fetchSchema(): Promise<OpenAPISchema> {
    try {
      const response = await fetch('/hms-mirror/v3/api-docs');
      if (!response.ok) {
        throw new Error(`Failed to fetch OpenAPI schema: ${response.statusText}`);
      }
      const schema = await response.json();
      return schema;
    } catch (error) {
      console.error('Error fetching OpenAPI schema:', error);
      return {};
    }
  }

  /**
   * Get the OpenAPI schema, fetching it if not already cached
   */
  private async getSchema(): Promise<OpenAPISchema> {
    if (this.schema) {
      return this.schema;
    }

    if (this.loading) {
      return this.loading;
    }

    this.loading = this.fetchSchema();
    this.schema = await this.loading;
    this.loading = null;

    return this.schema;
  }

  /**
   * Get the description for a specific field in a DTO class
   *
   * @param className - The DTO class name (e.g., "ConfigLiteDto", "ConnectionDto")
   * @param fieldName - The field name (e.g., "name", "description", "migrateNonNative")
   * @returns The field description from @Schema annotation, or undefined if not found
   */
  async getFieldDescription(className: string, fieldName: string): Promise<string | undefined> {
    const schema = await this.getSchema();

    if (!schema.components?.schemas) {
      return undefined;
    }

    const classSchema = schema.components.schemas[className];
    if (!classSchema?.properties) {
      return undefined;
    }

    const fieldSchema = classSchema.properties[fieldName];
    return fieldSchema?.description;
  }

  /**
   * Get all field descriptions for a DTO class
   *
   * @param className - The DTO class name
   * @returns Map of field names to their descriptions
   */
  async getClassDescriptions(className: string): Promise<Map<string, string>> {
    const schema = await this.getSchema();
    const descriptions = new Map<string, string>();

    if (!schema.components?.schemas) {
      return descriptions;
    }

    const classSchema = schema.components.schemas[className];
    if (!classSchema?.properties) {
      return descriptions;
    }

    Object.entries(classSchema.properties).forEach(([fieldName, fieldSchema]) => {
      if (fieldSchema.description) {
        descriptions.set(fieldName, fieldSchema.description);
      }
    });

    return descriptions;
  }

  /**
   * Clear the cached schema (useful for testing or if schema changes)
   */
  clearCache(): void {
    this.schema = null;
    this.loading = null;
  }
}

export const schemaService = new SchemaService();
export default schemaService;
