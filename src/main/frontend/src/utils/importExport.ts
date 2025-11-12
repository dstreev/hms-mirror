/**
 * Utility functions for importing and exporting JSON data
 */

/**
 * Downloads an object as a JSON file
 * @param data The object to export
 * @param filename The name of the file to download (without extension)
 */
export function exportToJson<T>(data: T, filename: string): void {
  try {
    // Convert object to JSON string with pretty formatting
    const jsonString = JSON.stringify(data, null, 2);

    // Create a Blob from the JSON string
    const blob = new Blob([jsonString], { type: 'application/json' });

    // Create a temporary URL for the blob
    const url = URL.createObjectURL(blob);

    // Create a temporary anchor element to trigger download
    const link = document.createElement('a');
    link.href = url;
    link.download = `${filename}.json`;

    // Trigger the download
    document.body.appendChild(link);
    link.click();

    // Cleanup
    document.body.removeChild(link);
    URL.revokeObjectURL(url);

    console.log(`Exported ${filename}.json successfully`);
  } catch (error) {
    console.error('Error exporting to JSON:', error);
    throw new Error('Failed to export data. Please try again.');
  }
}

/**
 * Reads a JSON file and parses it
 * @param file The file to read
 * @returns Promise that resolves to the parsed JSON object
 */
export function importFromJson<T>(file: File): Promise<T> {
  return new Promise((resolve, reject) => {
    // Validate file type
    if (!file.name.endsWith('.json')) {
      reject(new Error('Invalid file type. Please select a JSON file.'));
      return;
    }

    // Read the file
    const reader = new FileReader();

    reader.onload = (event) => {
      try {
        const content = event.target?.result as string;
        const data = JSON.parse(content) as T;
        resolve(data);
      } catch (error) {
        console.error('Error parsing JSON:', error);
        reject(new Error('Invalid JSON file. Please check the file format.'));
      }
    };

    reader.onerror = () => {
      reject(new Error('Failed to read file. Please try again.'));
    };

    reader.readAsText(file);
  });
}

/**
 * Generates a safe filename from a name and type
 * @param name The name of the item
 * @param type The type of item (connection, dataset, config)
 * @returns A safe filename string
 */
export function generateExportFilename(name: string, type: 'connection' | 'dataset' | 'config'): string {
  // Remove unsafe characters and replace spaces with underscores
  const safeName = name.replace(/[^a-zA-Z0-9-_]/g, '_');
  const timestamp = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
  return `${type}-${safeName}-${timestamp}`;
}

/**
 * Recursively masks password fields in an object by replacing values with '********'
 * @param obj The object to mask passwords in
 * @returns A deep copy of the object with passwords masked
 */
export function maskPasswords<T>(obj: T): T {
  if (obj === null || obj === undefined) {
    return obj;
  }

  // Handle primitive types
  if (typeof obj !== 'object') {
    return obj;
  }

  // Handle arrays
  if (Array.isArray(obj)) {
    return obj.map(item => maskPasswords(item)) as T;
  }

  // Handle objects
  const masked: any = {};
  for (const key in obj) {
    if (obj.hasOwnProperty(key)) {
      const value = (obj as any)[key];

      // Check if the key is 'password' (case-insensitive)
      if (key.toLowerCase() === 'password') {
        // Mask the password value
        masked[key] = value ? '********' : value;
      } else if (typeof value === 'object' && value !== null) {
        // Recursively process nested objects
        masked[key] = maskPasswords(value);
      } else {
        // Copy primitive values as-is
        masked[key] = value;
      }
    }
  }

  return masked as T;
}
