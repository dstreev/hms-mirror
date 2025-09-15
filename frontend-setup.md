# HMS-Mirror Frontend Setup Guide

## Project Structure for Big Bang Migration

```
hms-mirror/
├── src/main/
│   ├── frontend/                      # New React SPA
│   │   ├── public/
│   │   │   ├── index.html
│   │   │   ├── manifest.json
│   │   │   ├── sw.js
│   │   │   └── icons/
│   │   ├── src/
│   │   │   ├── components/
│   │   │   │   ├── cards/             # Card-based components
│   │   │   │   │   ├── ConfigCard.tsx
│   │   │   │   │   ├── ConnectCard.tsx
│   │   │   │   │   ├── ExecuteCard.tsx
│   │   │   │   │   ├── ReportCard.tsx
│   │   │   │   │   ├── BaseCard.tsx
│   │   │   │   │   └── CardGrid.tsx
│   │   │   │   ├── forms/             # Configuration forms
│   │   │   │   │   ├── ClusterForm.tsx
│   │   │   │   │   ├── FilterForm.tsx
│   │   │   │   │   └── MigrationForm.tsx
│   │   │   │   ├── common/            # Shared components
│   │   │   │   │   ├── Layout.tsx
│   │   │   │   │   ├── Header.tsx
│   │   │   │   │   ├── Navigation.tsx
│   │   │   │   │   └── LoadingSpinner.tsx
│   │   │   │   ├── dashboard/         # Dashboard views
│   │   │   │   │   ├── Dashboard.tsx
│   │   │   │   │   ├── StatusPanel.tsx
│   │   │   │   │   └── QuickActions.tsx
│   │   │   │   └── modals/            # Modal dialogs
│   │   │   ├── hooks/                 # Custom React hooks
│   │   │   │   ├── useWebSocket.ts
│   │   │   │   ├── useConfig.ts
│   │   │   │   ├── useNotifications.ts
│   │   │   │   └── usePWA.ts
│   │   │   ├── services/              # API services
│   │   │   │   ├── api/
│   │   │   │   │   ├── configApi.ts
│   │   │   │   │   ├── runtimeApi.ts
│   │   │   │   │   ├── reportsApi.ts
│   │   │   │   │   └── baseApi.ts
│   │   │   │   ├── websocket/
│   │   │   │   │   ├── WebSocketService.ts
│   │   │   │   │   └── eventHandlers.ts
│   │   │   │   └── storage/
│   │   │   │       ├── localStorage.ts
│   │   │   │       └── indexedDB.ts
│   │   │   ├── store/                 # Redux store
│   │   │   │   ├── slices/
│   │   │   │   │   ├── configSlice.ts
│   │   │   │   │   ├── runtimeSlice.ts
│   │   │   │   │   ├── uiSlice.ts
│   │   │   │   │   └── websocketSlice.ts
│   │   │   │   ├── thunks/
│   │   │   │   │   ├── configThunks.ts
│   │   │   │   │   └── runtimeThunks.ts
│   │   │   │   ├── selectors/
│   │   │   │   └── index.ts
│   │   │   ├── types/                 # TypeScript definitions
│   │   │   │   ├── api.ts
│   │   │   │   ├── config.ts
│   │   │   │   ├── runtime.ts
│   │   │   │   └── ui.ts
│   │   │   ├── utils/                 # Utility functions
│   │   │   │   ├── constants.ts
│   │   │   │   ├── helpers.ts
│   │   │   │   └── validation.ts
│   │   │   ├── styles/                # Styling
│   │   │   │   ├── globals.css
│   │   │   │   ├── cards.css
│   │   │   │   ├── components.css
│   │   │   │   └── animations.css
│   │   │   ├── App.tsx
│   │   │   ├── App.css
│   │   │   ├── index.tsx
│   │   │   └── index.css
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   ├── tailwind.config.js         # If using Tailwind
│   │   └── vite.config.ts             # If using Vite
│   │
│   ├── java/                          # Spring Boot backend (existing)
│   └── resources/
│       ├── static/                    # Will contain built React app
│       └── application.properties
│
├── pom.xml                           # Updated with frontend build
└── README.md
```

## Setup Commands

```bash
# 1. Navigate to project root
cd hms-mirror

# 2. Create frontend directory
mkdir -p src/main/frontend
cd src/main/frontend

# 3. Initialize React TypeScript project
npx create-react-app . --template typescript

# 4. Install additional dependencies
npm install @reduxjs/toolkit react-redux
npm install @types/react @types/react-dom
npm install axios
npm install socket.io-client @types/socket.io-client
npm install framer-motion  # For card animations
npm install react-router-dom @types/react-router-dom
npm install @heroicons/react  # For icons
npm install clsx  # For conditional classes
npm install react-hook-form @hookform/resolvers yup  # For forms

# 5. Install PWA dependencies
npm install workbox-webpack-plugin
npm install --save-dev @types/workbox-webpack-plugin

# 6. Install UI libraries (choose one)
# Option A: Tailwind CSS (Recommended)
npm install -D tailwindcss postcss autoprefixer
npx tailwindcss init -p

# Option B: Material-UI
npm install @mui/material @emotion/react @emotion/styled
npm install @mui/icons-material

# Option C: Ant Design
npm install antd
npm install @ant-design/icons
```

## Maven Configuration

Update `pom.xml` to include frontend build:

```xml
<plugin>
    <groupId>com.github.eirslett</groupId>
    <artifactId>frontend-maven-plugin</artifactId>
    <version>1.12.1</version>
    <configuration>
        <workingDirectory>src/main/frontend</workingDirectory>
        <installDirectory>target</installDirectory>
    </configuration>
    <executions>
        <execution>
            <id>install node and npm</id>
            <goals>
                <goal>install-node-and-npm</goal>
            </goals>
            <configuration>
                <nodeVersion>v18.17.0</nodeVersion>
                <npmVersion>9.6.7</npmVersion>
            </configuration>
        </execution>
        <execution>
            <id>npm install</id>
            <goals>
                <goal>npm</goal>
            </goals>
            <configuration>
                <arguments>install</arguments>
            </configuration>
        </execution>
        <execution>
            <id>npm run build</id>
            <goals>
                <goal>npm</goal>
            </goals>
            <configuration>
                <arguments>run build</arguments>
            </configuration>
        </execution>
    </executions>
</plugin>

<plugin>
    <artifactId>maven-resources-plugin</artifactId>
    <version>3.2.0</version>
    <executions>
        <execution>
            <id>copy-frontend-build</id>
            <phase>generate-resources</phase>
            <goals>
                <goal>copy-resources</goal>
            </goals>
            <configuration>
                <outputDirectory>${basedir}/src/main/resources/static</outputDirectory>
                <resources>
                    <resource>
                        <directory>src/main/frontend/build</directory>
                    </resource>
                </resources>
            </configuration>
        </execution>
    </executions>
</plugin>
```