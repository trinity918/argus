// Argus market-surveillance stack on Azure Container Apps.
//
// Topology (all inside one Container Apps environment):
//
//   [ingestor-binance] [ingestor-okx]  (live)  or  [replay]  (demo)
//            \              |                        /
//             +---------- nats (internal TCP 4222) -+
//                            |
//                        [detector] --(hash-chained audit)--> Azure Files share
//                            |                                      ^
//                     alerts / features                             | (read-only verify)
//                        /        \                                 |
//                    [ml]        [api] (external HTTPS) ------------+
//
// Deployed in two phases by deploy.sh because the app images live in the ACR
// this template creates:
//   phase 1: deployApps=false  -> registry, environment, storage, identity
//   phase 2: deployApps=true   -> the container apps, pulling from ACR via
//                                 user-assigned managed identity (no passwords)
targetScope = 'resourceGroup'

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Prefix for resource names. Keep short and alphanumeric (storage account names are capped at 24 chars).')
@maxLength(8)
param baseName string = 'argus'

@description('Phase switch: false deploys only infra (registry/env/storage), true adds the container apps.')
param deployApps bool = true

@description('Image tag for the argus and argus-ml images in ACR.')
param imageTag string = 'latest'

@description('demo replays the synthetic manipulation tape; live ingests real Binance + OKX feeds.')
@allowed(['demo', 'live'])
param feedMode string = 'demo'

@description('Binance symbols for live mode.')
param binanceSymbols string = 'BTCUSDT,ETHUSDT'

@description('OKX instIds for live mode.')
param okxSymbols string = 'BTC-USDT,ETH-USDT'

var suffix = uniqueString(resourceGroup().id)
var acrName = toLower('${baseName}acr${suffix}')
var storageName = toLower('${baseName}sa${suffix}')
var shareName = 'audit'

// ---------- observability ----------

resource logs 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: '${baseName}-logs'
  location: location
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
  }
}

// ---------- registry + pull identity ----------

resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: location
  sku: { name: 'Basic' }
  properties: {
    adminUserEnabled: false // pulls use managed identity, not passwords
  }
}

resource mi 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${baseName}-mi'
  location: location
}

// AcrPull (7f951dda-4ed3-4680-a7ca-43fe172d538d) for the identity on this ACR.
resource acrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: acr
  name: guid(acr.id, mi.id, 'acrpull')
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      '7f951dda-4ed3-4680-a7ca-43fe172d538d'
    )
    principalId: mi.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// ---------- audit-trail storage (Azure Files, shared detector -> api) ----------

resource sa 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource fileService 'Microsoft.Storage/storageAccounts/fileServices@2023-01-01' = {
  parent: sa
  name: 'default'
}

resource share 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-01-01' = {
  parent: fileService
  name: shareName
  properties: { shareQuota: 16 }
}

// ---------- container apps environment ----------

resource env 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: '${baseName}-env'
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logs.properties.customerId
        sharedKey: logs.listKeys().primarySharedKey
      }
    }
  }
}

resource envStorage 'Microsoft.App/managedEnvironments/storages@2024-03-01' = {
  parent: env
  name: shareName
  properties: {
    azureFile: {
      accountName: sa.name
      accountKey: sa.listKeys().keys[0].value
      shareName: shareName
      accessMode: 'ReadWrite'
    }
  }
  dependsOn: [share]
}

// ---------- shared app config ----------

var registryConfig = [
  {
    server: acr.properties.loginServer
    identity: mi.id
  }
]
var appIdentity = {
  type: 'UserAssigned'
  userAssignedIdentities: { '${mi.id}': {} }
}
var argusImage = '${acr.properties.loginServer}/argus:${imageTag}'
var mlImage = '${acr.properties.loginServer}/argus-ml:${imageTag}'

// ---------- nats (internal TCP) ----------

resource natsApp 'Microsoft.App/containerApps@2024-03-01' = if (deployApps) {
  name: '${baseName}-nats'
  location: location
  properties: {
    managedEnvironmentId: env.id
    configuration: {
      ingress: {
        external: false
        transport: 'tcp'
        targetPort: 4222
        exposedPort: 4222
      }
    }
    template: {
      containers: [
        {
          name: 'nats'
          image: 'docker.io/library/nats:2.10-alpine'
          args: ['-js']
          resources: { cpu: json('0.5'), memory: '1Gi' }
        }
      ]
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
}

// natsApp is guaranteed non-null when deployApps is true (same condition).
var natsUrl = deployApps ? 'nats://${natsApp!.properties.configuration.ingress.fqdn}:4222' : ''

// ---------- detector ----------
// maxReplicas is deliberately 1: the hash-chained audit log is single-writer by
// design (each entry commits to the previous hash). Horizontal scale-out is done
// by running additional detector apps with disjoint -subjects filters and their
// own audit directories, not by replicating this one.

resource detectorApp 'Microsoft.App/containerApps@2024-03-01' = if (deployApps) {
  name: '${baseName}-detector'
  location: location
  identity: appIdentity
  properties: {
    managedEnvironmentId: env.id
    configuration: {
      registries: registryConfig
    }
    template: {
      containers: [
        {
          name: 'detector'
          image: argusImage
          command: ['detector']
          args: ['-nats', natsUrl, '-audit-dir', '/data/audit', '-metrics-addr', ':2113']
          resources: { cpu: json('1.0'), memory: '2Gi' }
          volumeMounts: [{ volumeName: 'audit', mountPath: '/data' }]
        }
      ]
      volumes: [
        {
          name: 'audit'
          storageType: 'AzureFile'
          storageName: envStorage.name
        }
      ]
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
}

// ---------- api / dashboard (external HTTPS) ----------

resource apiApp 'Microsoft.App/containerApps@2024-03-01' = if (deployApps) {
  name: '${baseName}-api'
  location: location
  identity: appIdentity
  properties: {
    managedEnvironmentId: env.id
    configuration: {
      registries: registryConfig
      ingress: {
        external: true
        targetPort: 8080
        transport: 'auto'
      }
    }
    template: {
      containers: [
        {
          name: 'api'
          image: argusImage
          command: ['api']
          args: ['-nats', natsUrl, '-audit-dir', '/data/audit', '-addr', ':8080']
          resources: { cpu: json('0.5'), memory: '1Gi' }
          volumeMounts: [{ volumeName: 'audit', mountPath: '/data' }]
        }
      ]
      volumes: [
        {
          name: 'audit'
          storageType: 'AzureFile'
          storageName: envStorage.name
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 3
        rules: [
          {
            name: 'http'
            http: { metadata: { concurrentRequests: '100' } }
          }
        ]
      }
    }
  }
  dependsOn: [detectorApp]
}

// ---------- ml scorer ----------

resource mlApp 'Microsoft.App/containerApps@2024-03-01' = if (deployApps) {
  name: '${baseName}-ml'
  location: location
  identity: appIdentity
  properties: {
    managedEnvironmentId: env.id
    configuration: {
      registries: registryConfig
    }
    template: {
      containers: [
        {
          name: 'ml'
          image: mlImage
          env: [{ name: 'NATS_URL', value: natsUrl }]
          resources: { cpu: json('0.5'), memory: '1Gi' }
        }
      ]
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
}

// ---------- feed: synthetic replay (demo) or live ingestors ----------

resource replayApp 'Microsoft.App/containerApps@2024-03-01' = if (deployApps && feedMode == 'demo') {
  name: '${baseName}-replay'
  location: location
  identity: appIdentity
  properties: {
    managedEnvironmentId: env.id
    configuration: {
      registries: registryConfig
    }
    template: {
      containers: [
        {
          name: 'replay'
          image: argusImage
          command: ['replay']
          args: ['-nats', natsUrl, '-symbol', 'BTCUSDT', '-speed', '2']
          resources: { cpu: json('0.25'), memory: '0.5Gi' }
        }
      ]
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
}

resource ingestorBinance 'Microsoft.App/containerApps@2024-03-01' = if (deployApps && feedMode == 'live') {
  name: '${baseName}-ingest-binance'
  location: location
  identity: appIdentity
  properties: {
    managedEnvironmentId: env.id
    configuration: {
      registries: registryConfig
    }
    template: {
      containers: [
        {
          name: 'ingestor'
          image: argusImage
          command: ['ingestor']
          args: ['-exchange', 'binance', '-symbols', binanceSymbols, '-nats', natsUrl, '-metrics-addr', ':2112']
          resources: { cpu: json('0.5'), memory: '1Gi' }
        }
      ]
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
}

resource ingestorOkx 'Microsoft.App/containerApps@2024-03-01' = if (deployApps && feedMode == 'live') {
  name: '${baseName}-ingest-okx'
  location: location
  identity: appIdentity
  properties: {
    managedEnvironmentId: env.id
    configuration: {
      registries: registryConfig
    }
    template: {
      containers: [
        {
          name: 'ingestor'
          image: argusImage
          command: ['ingestor']
          args: ['-exchange', 'okx', '-symbols', okxSymbols, '-nats', natsUrl, '-metrics-addr', ':2114']
          resources: { cpu: json('0.5'), memory: '1Gi' }
        }
      ]
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
}

// ---------- outputs ----------

output acrName string = acr.name
output acrLoginServer string = acr.properties.loginServer
output environmentName string = env.name
output dashboardUrl string = deployApps ? 'https://${apiApp!.properties.configuration.ingress.fqdn}' : ''
