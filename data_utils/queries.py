# DataHub GraphQL 쿼리 모음

ROOT_GLOSSARY_NODES_QUERY = """
query getRootGlossaryNodes {
  getRootGlossaryNodes(input: {start: 0, count: 1000}) {
    count
    start
    total
    nodes {
      ...rootGlossaryNodeWithFourLayers
      __typename
    }
    __typename
  }
}

fragment rootGlossaryNodeWithFourLayers on GlossaryNode {
  urn
  type
  properties {
    name
    description
    __typename
  }
  displayProperties {
    ...displayPropertiesFields
    __typename
  }
  children: relationships(
    input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 500}
  ) {
    total
    relationships {
      type
      entity {
        type
        ... on GlossaryTerm {
          urn
          __typename
        }
        ... on GlossaryNode {
          urn
          children: relationships(
            input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 500}
          ) {
            total
            relationships {
              type
              entity {
                type
                ... on GlossaryTerm {
                  urn
                  __typename
                }
                ... on GlossaryNode {
                  urn
                  children: relationships(
                    input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 500}
                  ) {
                    total
                    relationships {
                      type
                      entity {
                        type
                        ... on GlossaryTerm {
                          urn
                          __typename
                        }
                        ... on GlossaryNode {
                          urn
                          children: relationships(
                            input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 500}
                          ) {
                            total
                            relationships {
                              type
                              entity {
                                type
                                ... on GlossaryTerm {
                                  urn
                                  __typename
                                }
                                ... on GlossaryNode {
                                  urn
                                  __typename
                                }
                                __typename
                              }
                              __typename
                            }
                            __typename
                          }
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment displayPropertiesFields on DisplayProperties {
  colorHex
  icon {
    name
    style
    iconLibrary
    __typename
  }
  __typename
}
"""

GLOSSARY_NODE_QUERY = """
query getGlossaryNode($urn: String!) {
  glossaryNode(urn: $urn) {
    urn
    type
    exists
    properties {
      name
      description
      __typename
    }
    ownership {
      ...ownershipFields
      __typename
    }
    parentNodes {
      ...parentNodesFields
      __typename
    }
    privileges {
      ...entityPrivileges
      __typename
    }
    autoRenderAspects: aspects(input: {autoRenderOnly: true}) {
      ...autoRenderAspectFields
      __typename
    }
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    forms {
      ...formsFields
      __typename
    }
    children: relationships(
      input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 10000}
    ) {
      total
      relationships {
        direction
        entity {
          type
          urn
          ... on GlossaryNode {
            ...glossaryNode
            __typename
          }
          ... on GlossaryTerm {
            ...childGlossaryTerm
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    displayProperties {
      ...displayPropertiesFields
      __typename
    }
    ...notes
    __typename
  }
}

fragment ownershipFields on Ownership {
  owners {
    owner {
      ... on CorpUser {
        urn
        type
        username
        info {
          active
          displayName
          title
          email
          firstName
          lastName
          fullName
          __typename
        }
        properties {
          active
          displayName
          title
          email
          firstName
          lastName
          fullName
          __typename
        }
        editableProperties {
          displayName
          title
          pictureLink
          email
          __typename
        }
        __typename
      }
      ... on CorpGroup {
        urn
        type
        name
        properties {
          displayName
          email
          __typename
        }
        info {
          displayName
          email
          admins {
            urn
            username
            info {
              active
              displayName
              title
              email
              firstName
              lastName
              fullName
              __typename
            }
            editableInfo {
              pictureLink
              teams
              skills
              __typename
            }
            __typename
          }
          members {
            urn
            username
            info {
              active
              displayName
              title
              email
              firstName
              lastName
              fullName
              __typename
            }
            editableInfo {
              pictureLink
              teams
              skills
              __typename
            }
            __typename
          }
          groups
          __typename
        }
        __typename
      }
      __typename
    }
    type
    ownershipType {
      urn
      type
      info {
        name
        description
        __typename
      }
      status {
        removed
        __typename
      }
      __typename
    }
    associatedUrn
    __typename
  }
  lastModified {
    time
    __typename
  }
  __typename
}

fragment parentNodesFields on ParentNodesResult {
  count
  nodes {
    urn
    type
    properties {
      name
      __typename
    }
    displayProperties {
      ...displayPropertiesFields
      __typename
    }
    __typename
  }
  __typename
}

fragment displayPropertiesFields on DisplayProperties {
  colorHex
  icon {
    name
    style
    iconLibrary
    __typename
  }
  __typename
}

fragment entityPrivileges on EntityPrivileges {
  canEditLineage
  canEditDomains
  canEditDataProducts
  canEditTags
  canEditGlossaryTerms
  canEditDescription
  canEditLinks
  canEditOwners
  canEditAssertions
  canEditIncidents
  canEditDeprecation
  canEditSchemaFieldTags
  canEditSchemaFieldGlossaryTerms
  canEditSchemaFieldDescription
  canEditQueries
  canEditEmbed
  canManageEntity
  canManageChildren
  canEditProperties
  canViewDatasetUsage
  canViewDatasetProfile
  canViewDatasetOperations
  __typename
}

fragment autoRenderAspectFields on RawAspect {
  aspectName
  payload
  renderSpec {
    displayType
    displayName
    key
    __typename
  }
  __typename
}

fragment structuredPropertiesFields on StructuredPropertiesEntry {
  structuredProperty {
    exists
    ...structuredPropertyFields
    __typename
  }
  values {
    ... on StringValue {
      stringValue
      __typename
    }
    ... on NumberValue {
      numberValue
      __typename
    }
    __typename
  }
  valueEntities {
    urn
    type
    ...entityDisplayNameFields
    __typename
  }
  associatedUrn
  __typename
}

fragment structuredPropertyFields on StructuredPropertyEntity {
  urn
  type
  definition {
    displayName
    qualifiedName
    description
    cardinality
    immutable
    valueType {
      urn
      type
      info {
        type
        displayName
        __typename
      }
      __typename
    }
    entityTypes {
      urn
      type
      info {
        type
        __typename
      }
      __typename
    }
    cardinality
    typeQualifier {
      allowedTypes {
        urn
        type
        info {
          type
          displayName
          __typename
        }
        __typename
      }
      __typename
    }
    allowedValues {
      value {
        ... on StringValue {
          stringValue
          __typename
        }
        ... on NumberValue {
          numberValue
          __typename
        }
        __typename
      }
      description
      __typename
    }
    created {
      time
      actor {
        urn
        editableProperties {
          displayName
          pictureLink
          __typename
        }
        ...entityDisplayNameFields
        __typename
      }
      __typename
    }
    lastModified {
      time
      actor {
        urn
        editableProperties {
          displayName
          pictureLink
          __typename
        }
        ...entityDisplayNameFields
        __typename
      }
      __typename
    }
    __typename
  }
  settings {
    isHidden
    showInSearchFilters
    showAsAssetBadge
    showInAssetSummary
    showInColumnsTable
    __typename
  }
  __typename
}

fragment entityDisplayNameFields on Entity {
  urn
  type
  ... on Dataset {
    name
    properties {
      name
      qualifiedName
      __typename
    }
    __typename
  }
  ... on CorpUser {
    username
    properties {
      displayName
      title
      firstName
      lastName
      fullName
      email
      __typename
    }
    info {
      active
      displayName
      title
      firstName
      lastName
      fullName
      email
      __typename
    }
    __typename
  }
  ... on CorpGroup {
    name
    info {
      displayName
      __typename
    }
    __typename
  }
  ... on Dashboard {
    dashboardId
    properties {
      name
      __typename
    }
    __typename
  }
  ... on Chart {
    chartId
    properties {
      name
      __typename
    }
    __typename
  }
  ... on DataFlow {
    properties {
      name
      __typename
    }
    __typename
  }
  ... on DataJob {
    jobId
    properties {
      name
      __typename
    }
    __typename
  }
  ... on GlossaryTerm {
    name
    hierarchicalName
    properties {
      name
      __typename
    }
    __typename
  }
  ... on GlossaryNode {
    properties {
      name
      description
      __typename
    }
    __typename
  }
  ... on Domain {
    properties {
      name
      __typename
    }
    __typename
  }
  ... on Container {
    properties {
      name
      __typename
    }
    __typename
  }
  ... on MLFeatureTable {
    name
    __typename
  }
  ... on MLFeature {
    name
    __typename
  }
  ... on MLPrimaryKey {
    name
    __typename
  }
  ... on MLModel {
    name
    __typename
  }
  ... on MLModelGroup {
    name
    __typename
  }
  ... on Tag {
    name
    properties {
      name
      colorHex
      __typename
    }
    __typename
  }
  ... on DataPlatform {
    ...nonConflictingPlatformFields
    __typename
  }
  ... on DataProduct {
    properties {
      name
      __typename
    }
    __typename
  }
  ... on DataPlatformInstance {
    instanceId
    __typename
  }
  ... on StructuredPropertyEntity {
    definition {
      displayName
      qualifiedName
      __typename
    }
    __typename
  }
  ... on SchemaFieldEntity {
    fieldPath
    __typename
  }
  ... on OwnershipTypeEntity {
    info {
      name
      __typename
    }
    __typename
  }
  __typename
}

fragment nonConflictingPlatformFields on DataPlatform {
  urn
  type
  name
  properties {
    displayName
    datasetNameDelimiter
    logoUrl
    __typename
  }
  displayName
  info {
    type
    displayName
    datasetNameDelimiter
    logoUrl
    __typename
  }
  __typename
}

fragment formsFields on Forms {
  completedForms {
    ...formAssociationFields
    __typename
  }
  incompleteForms {
    ...formAssociationFields
    __typename
  }
  verifications {
    form {
      urn
      __typename
    }
    lastModified {
      time
      actor {
        urn
        type
        ...entityDisplayNameFields
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment formAssociationFields on FormAssociation {
  associatedUrn
  incompletePrompts {
    ...formPromptAssociationFields
    __typename
  }
  completedPrompts {
    ...formPromptAssociationFields
    __typename
  }
  form {
    urn
    type
    info {
      name
      description
      type
      prompts {
        id
        formUrn
        title
        description
        type
        required
        structuredPropertyParams {
          structuredProperty {
            ...structuredPropertyFields
            __typename
          }
          __typename
        }
        __typename
      }
      actors {
        owners
        isAssignedToMe
        __typename
      }
      __typename
    }
    ownership {
      ...ownershipFields
      __typename
    }
    __typename
  }
  __typename
}

fragment formPromptAssociationFields on FormPromptAssociation {
  id
  lastModified {
    time
    actor {
      urn
      type
      ...entityDisplayNameFields
      __typename
    }
    __typename
  }
  fieldAssociations {
    completedFieldPrompts {
      fieldPath
      lastModified {
        time
        actor {
          urn
          type
          ...entityDisplayNameFields
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment glossaryNode on GlossaryNode {
  urn
  type
  properties {
    name
    description
    __typename
  }
  displayProperties {
    ...displayPropertiesFields
    __typename
  }
  children: relationships(
    input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 1000}
  ) {
    total
    relationships {
      type
      entity {
        ... on GlossaryTerm {
          urn
          name
          type
          hierarchicalName
          properties {
            name
            description
            __typename
          }
          __typename
        }
        ... on GlossaryNode {
          urn
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment childGlossaryTerm on GlossaryTerm {
  urn
  type
  name
  hierarchicalName
  properties {
    name
    description
    __typename
  }
  __typename
}

fragment notes on Entity {
  notes: relationships(
    input: {types: ["PostTarget"], direction: INCOMING, start: 0, count: 100}
  ) {
    total
    relationships {
      type
      entity {
        ... on Post {
          urn
          type
          postType
          lastModified {
            time
            actor
            __typename
          }
          content {
            contentType
            title
            description
            link
            media {
              type
              location
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}
"""

LIST_QUERIES_QUERY = """
query listQueries($input: ListQueriesInput!) {
  listQueries(input: $input) {
    start
    total
    count
    queries {
      ...query
      __typename
    }
    __typename
  }
}

fragment query on QueryEntity {
  urn
  properties {
    name
    description
    source
    statement {
      value
      language
      __typename
    }
    created {
      time
      actor
      __typename
    }
    lastModified {
      time
      actor
      __typename
    }
    origin {
      ...searchResultFields
      __typename
    }
    __typename
  }
  platform {
    ...platformFields
    __typename
  }
  subjects {
    dataset {
      urn
      type
      name
      __typename
    }
    schemaField {
      urn
      type
      fieldPath
      __typename
    }
    __typename
  }
  __typename
}

fragment searchResultFields on Entity {
  ...searchResultFieldsNoLineage
  ... on EntityWithRelationships {
    upstream: lineage(input: {direction: UPSTREAM, start: 0, count: 100}) {
      total
      filtered
      __typename
    }
    downstream: lineage(input: {direction: DOWNSTREAM, start: 0, count: 100}) {
      total
      filtered
      __typename
    }
    __typename
  }
  __typename
}

fragment searchResultFieldsNoLineage on Entity {
  ...searchResultsWithoutSchemaField
  ... on SchemaFieldEntity {
    ...entityField
    __typename
  }
  __typename
}

fragment searchResultsWithoutSchemaField on Entity {
  urn
  type
  ... on Dataset {
    ...nonSiblingsDatasetSearchFields
    siblings {
      isPrimary
      siblings {
        urn
        type
        ... on Dataset {
          ...nonSiblingsDatasetSearchFields
          structuredProperties {
            properties {
              ...structuredPropertiesFields
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    siblingsSearch(input: {query: "*", count: 1}) {
      count
      total
      searchResults {
        entity {
          urn
          type
          ... on Dataset {
            ...nonSiblingsDatasetSearchFields
            siblings {
              isPrimary
              __typename
            }
            structuredProperties {
              properties {
                ...structuredPropertiesFields
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    browsePathV2 {
      ...browsePathV2Fields
      __typename
    }
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    __typename
  }
  ... on DataPlatformInstance {
    ...dataPlatformInstanceFields
    __typename
  }
  ... on Role {
    id
    properties {
      name
      description
      __typename
    }
    __typename
  }
  ... on CorpUser {
    username
    properties {
      active
      displayName
      title
      firstName
      lastName
      fullName
      email
      departmentName
      title
      __typename
    }
    info {
      active
      displayName
      title
      firstName
      lastName
      fullName
      email
      departmentName
      title
      __typename
    }
    editableProperties {
      displayName
      title
      pictureLink
      __typename
    }
    __typename
  }
  ... on CorpGroup {
    name
    info {
      displayName
      description
      __typename
    }
    memberCount: relationships(
      input: {types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"], direction: INCOMING, start: 0, count: 1}
    ) {
      total
      __typename
    }
    __typename
  }
  ... on Dashboard {
    tool
    dashboardId
    properties {
      name
      description
      externalUrl
      access
      lastModified {
        time
        __typename
      }
      __typename
    }
    ownership {
      ...ownershipFields
      __typename
    }
    globalTags {
      ...globalTagsFields
      __typename
    }
    glossaryTerms {
      ...glossaryTerms
      __typename
    }
    editableProperties {
      description
      __typename
    }
    platform {
      ...platformFields
      __typename
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
      __typename
    }
    domain {
      ...entityDomain
      __typename
    }
    ...entityDataProduct
    deprecation {
      ...deprecationFields
      __typename
    }
    parentContainers {
      ...parentContainersFields
      __typename
    }
    statsSummary {
      viewCount
      uniqueUserCountLast30Days
      topUsersLast30Days {
        urn
        type
        username
        properties {
          displayName
          firstName
          lastName
          fullName
          __typename
        }
        editableProperties {
          displayName
          pictureLink
          __typename
        }
        __typename
      }
      __typename
    }
    subTypes {
      typeNames
      __typename
    }
    health {
      ...entityHealth
      __typename
    }
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    browsePathV2 {
      ...browsePathV2Fields
      __typename
    }
    __typename
  }
  ... on Chart {
    chartId
    properties {
      name
      description
      externalUrl
      type
      access
      lastModified {
        time
        __typename
      }
      created {
        time
        __typename
      }
      __typename
    }
    ownership {
      ...ownershipFields
      __typename
    }
    globalTags {
      ...globalTagsFields
      __typename
    }
    glossaryTerms {
      ...glossaryTerms
      __typename
    }
    editableProperties {
      description
      __typename
    }
    platform {
      ...platformFields
      __typename
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
      __typename
    }
    domain {
      ...entityDomain
      __typename
    }
    ...entityDataProduct
    deprecation {
      ...deprecationFields
      __typename
    }
    parentContainers {
      ...parentContainersFields
      __typename
    }
    browsePathV2 {
      ...browsePathV2Fields
      __typename
    }
    statsSummary {
      viewCount
      uniqueUserCountLast30Days
      topUsersLast30Days {
        urn
        type
        username
        properties {
          displayName
          firstName
          lastName
          fullName
          __typename
        }
        editableProperties {
          displayName
          pictureLink
          __typename
        }
        __typename
      }
      __typename
    }
    subTypes {
      typeNames
      __typename
    }
    health {
      ...entityHealth
      __typename
    }
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    __typename
  }
  ... on DataFlow {
    orchestrator
    flowId
    cluster
    properties {
      name
      description
      project
      externalUrl
      __typename
    }
    ownership {
      ...ownershipFields
      __typename
    }
    globalTags {
      ...globalTagsFields
      __typename
    }
    glossaryTerms {
      ...glossaryTerms
      __typename
    }
    editableProperties {
      description
      __typename
    }
    platform {
      ...platformFields
      __typename
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
      __typename
    }
    parentContainers {
      ...parentContainersFields
      __typename
    }
    domain {
      ...entityDomain
      __typename
    }
    ...entityDataProduct
    deprecation {
      ...deprecationFields
      __typename
    }
    childJobs: relationships(
      input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 100}
    ) {
      total
      __typename
    }
    health {
      ...entityHealth
      __typename
    }
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    __typename
  }
  ... on DataJob {
    dataFlow {
      ...nonRecursiveDataFlowFields
      __typename
    }
    jobId
    ownership {
      ...ownershipFields
      __typename
    }
    properties {
      name
      description
      externalUrl
      __typename
    }
    globalTags {
      ...globalTagsFields
      __typename
    }
    glossaryTerms {
      ...glossaryTerms
      __typename
    }
    editableProperties {
      description
      __typename
    }
    domain {
      ...entityDomain
      __typename
    }
    ...entityDataProduct
    deprecation {
      ...deprecationFields
      __typename
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
      __typename
    }
    parentContainers {
      ...parentContainersFields
      __typename
    }
    subTypes {
      typeNames
      __typename
    }
    lastRun: runs(start: 0, count: 1) {
      count
      start
      total
      runs {
        urn
        type
        created {
          time
          actor
          __typename
        }
        __typename
      }
      __typename
    }
    health {
      ...entityHealth
      __typename
    }
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    __typename
  }
  ... on GlossaryTerm {
    name
    hierarchicalName
    properties {
      name
      description
      termSource
      sourceRef
      sourceUrl
      rawSchema
      customProperties {
        key
        value
        __typename
      }
      __typename
    }
    deprecation {
      ...deprecationFields
      __typename
    }
    parentNodes {
      ...parentNodesFields
      __typename
    }
    domain {
      ...entityDomain
      __typename
    }
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    __typename
  }
  ... on GlossaryNode {
    ...glossaryNode
    parentNodes {
      ...parentNodesFields
      __typename
    }
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    __typename
  }
  ... on Domain {
    properties {
      name
      description
      __typename
    }
    ownership {
      ...ownershipFields
      __typename
    }
    parentDomains {
      ...parentDomainsFields
      __typename
    }
    displayProperties {
      ...displayPropertiesFields
      __typename
    }
    ...domainEntitiesFields
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    __typename
  }
  ... on Container {
    properties {
      name
      description
      externalUrl
      __typename
    }
    platform {
      ...platformFields
      __typename
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
      __typename
    }
    editableProperties {
      description
      __typename
    }
    ownership {
      ...ownershipFields
      __typename
    }
    tags {
      ...globalTagsFields
      __typename
    }
    glossaryTerms {
      ...glossaryTerms
      __typename
    }
    subTypes {
      typeNames
      __typename
    }
    entities(input: {}) {
      total
      __typename
    }
    deprecation {
      ...deprecationFields
      __typename
    }
    parentContainers {
      ...parentContainersFields
      __typename
    }
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    __typename
  }
  ... on MLFeatureTable {
    name
    description
    featureTableProperties {
      description
      mlFeatures {
        urn
        __typename
      }
      mlPrimaryKeys {
        urn
        __typename
      }
      __typename
    }
    ownership {
      ...ownershipFields
      __typename
    }
    platform {
      ...platformFields
      __typename
    }
    deprecation {
      ...deprecationFields
      __typename
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
      __typename
    }
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    __typename
  }
  ... on MLFeature {
    ...nonRecursiveMLFeature
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    __typename
  }
  ... on MLPrimaryKey {
    ...nonRecursiveMLPrimaryKey
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    __typename
  }
  ... on MLModel {
    name
    description
    origin
    ownership {
      ...ownershipFields
      __typename
    }
    platform {
      ...platformFields
      __typename
    }
    deprecation {
      ...deprecationFields
      __typename
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
      __typename
    }
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    properties {
      propertiesName: name
      __typename
    }
    __typename
  }
  ... on MLModelGroup {
    name
    origin
    description
    ownership {
      ...ownershipFields
      __typename
    }
    platform {
      ...platformFields
      __typename
    }
    deprecation {
      ...deprecationFields
      __typename
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
      __typename
    }
    structuredProperties {
      properties {
        ...structuredPropertiesFields
        __typename
      }
      __typename
    }
    properties {
      propertiesName: name
      __typename
    }
    __typename
  }
  ... on Tag {
    name
    properties {
      name
      colorHex
      __typename
    }
    description
    __typename
  }
  ... on DataPlatform {
    ...nonConflictingPlatformFields
    __typename
  }
  ... on DataProduct {
    ...dataProductSearchFields
    __typename
  }
  ... on ERModelRelationship {
    urn
    type
    id
    properties {
      ...ermodelrelationPropertiesFields
      __typename
    }
    editableProperties {
      ...ermodelrelationEditablePropertiesFields
      __typename
    }
    ownership {
      ...ownershipFields
      __typename
    }
    tags {
      ...globalTagsFields
      __typename
    }
    glossaryTerms {
      ...glossaryTerms
      __typename
    }
    __typename
  }
  ... on BusinessAttribute {
    ...businessAttributeFields
    __typename
  }
  ... on StructuredPropertyEntity {
    ...structuredPropertyFields
    __typename
  }
  ... on SupportsVersions {
    versionProperties {
      ...versionProperties
      __typename
    }
    __typename
  }
  ... on DataProcessInstance {
    ...dataProcessInstanceFields
    __typename
  }
  ... on DataPlatformInstance {
    ...dataPlatformInstanceFields
    __typename
  }
  __typename
}

fragment nonSiblingsDatasetSearchFields on Dataset {
  exists
  name
  origin
  uri
  platform {
    ...platformFields
    __typename
  }
  dataPlatformInstance {
    ...dataPlatformInstanceFields
    __typename
  }
  editableProperties {
    name
    description
    __typename
  }
  access {
    ...getAccess
    __typename
  }
  platformNativeType
  properties {
    name
    description
    qualifiedName
    customProperties {
      key
      value
      __typename
    }
    externalUrl
    lastModified {
      time
      actor
      __typename
    }
    __typename
  }
  ownership {
    ...ownershipFields
    __typename
  }
  globalTags {
    ...globalTagsFields
    __typename
  }
  glossaryTerms {
    ...glossaryTerms
    __typename
  }
  subTypes {
    typeNames
    __typename
  }
  domain {
    ...entityDomain
    __typename
  }
  ...entityDataProduct
  parentContainers {
    ...parentContainersFields
    __typename
  }
  deprecation {
    ...deprecationFields
    __typename
  }
  health {
    ...entityHealth
    __typename
  }
  ...datasetStatsFields
  __typename
}

fragment platformFields on DataPlatform {
  urn
  type
  lastIngested
  name
  properties {
    type
    displayName
    datasetNameDelimiter
    logoUrl
    __typename
  }
  displayName
  info {
    type
    displayName
    datasetNameDelimiter
    logoUrl
    __typename
  }
  __typename
}

fragment dataPlatformInstanceFields on DataPlatformInstance {
  urn
  type
  platform {
    ...platformFields
    __typename
  }
  instanceId
  __typename
}

fragment getAccess on Access {
  roles {
    role {
      ...getRolesName
      __typename
    }
    __typename
  }
  __typename
}

fragment getRolesName on Role {
  urn
  type
  id
  properties {
    name
    description
    type
    __typename
  }
  __typename
}

fragment ownershipFields on Ownership {
  owners {
    owner {
      ... on CorpUser {
        urn
        type
        username
        info {
          active
          displayName
          title
          email
          firstName
          lastName
          fullName
          __typename
        }
        properties {
          active
          displayName
          title
          email
          firstName
          lastName
          fullName
          __typename
        }
        editableProperties {
          displayName
          title
          pictureLink
          email
          __typename
        }
        __typename
      }
      ... on CorpGroup {
        urn
        type
        name
        properties {
          displayName
          email
          __typename
        }
        info {
          displayName
          email
          admins {
            urn
            username
            info {
              active
              displayName
              title
              email
              firstName
              lastName
              fullName
              __typename
            }
            editableInfo {
              pictureLink
              teams
              skills
              __typename
            }
            __typename
          }
          members {
            urn
            username
            info {
              active
              displayName
              title
              email
              firstName
              lastName
              fullName
              __typename
            }
            editableInfo {
              pictureLink
              teams
              skills
              __typename
            }
            __typename
          }
          groups
          __typename
        }
        __typename
      }
      __typename
    }
    type
    ownershipType {
      urn
      type
      info {
        name
        description
        __typename
      }
      status {
        removed
        __typename
      }
      __typename
    }
    associatedUrn
    __typename
  }
  lastModified {
    time
    __typename
  }
  __typename
}

fragment globalTagsFields on GlobalTags {
  tags {
    tag {
      urn
      type
      name
      description
      properties {
        name
        colorHex
        __typename
      }
      __typename
    }
    associatedUrn
    __typename
  }
  __typename
}

fragment glossaryTerms on GlossaryTerms {
  terms {
    term {
      ...glossaryTerm
      __typename
    }
    actor {
      urn
      __typename
    }
    associatedUrn
    __typename
  }
  __typename
}

fragment glossaryTerm on GlossaryTerm {
  urn
  name
  type
  hierarchicalName
  properties {
    name
    description
    definition
    termSource
    customProperties {
      key
      value
      __typename
    }
    __typename
  }
  ownership {
    ...ownershipFields
    __typename
  }
  parentNodes {
    ...parentNodesFields
    __typename
  }
  __typename
}

fragment parentNodesFields on ParentNodesResult {
  count
  nodes {
    urn
    type
    properties {
      name
      __typename
    }
    displayProperties {
      ...displayPropertiesFields
      __typename
    }
    __typename
  }
  __typename
}

fragment displayPropertiesFields on DisplayProperties {
  colorHex
  icon {
    name
    style
    iconLibrary
    __typename
  }
  __typename
}

fragment entityDomain on DomainAssociation {
  domain {
    urn
    type
    properties {
      name
      description
      __typename
    }
    parentDomains {
      ...parentDomainsFields
      __typename
    }
    ...domainEntitiesFields
    displayProperties {
      ...displayPropertiesFields
      __typename
    }
    __typename
  }
  associatedUrn
  __typename
}

fragment parentDomainsFields on ParentDomainsResult {
  count
  domains {
    urn
    type
    ... on Domain {
      displayProperties {
        ...displayPropertiesFields
        __typename
      }
      properties {
        name
        description
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment domainEntitiesFields on Domain {
  entities(input: {start: 0, count: 0}) {
    total
    __typename
  }
  dataProducts: entities(
    input: {start: 0, count: 0, filters: [{field: "_entityType", values: "DATA_PRODUCT"}]}
  ) {
    total
    __typename
  }
  children: relationships(
    input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 0}
  ) {
    total
    __typename
  }
  __typename
}

fragment entityDataProduct on Entity {
  dataProduct: relationships(
    input: {types: ["DataProductContains"], direction: INCOMING, start: 0, count: 1}
  ) {
    relationships {
      type
      entity {
        urn
        type
        ... on DataProduct {
          properties {
            name
            description
            __typename
          }
          domain {
            ...entityDomain
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment parentContainersFields on ParentContainersResult {
  count
  containers {
    ...parentContainerFields
    __typename
  }
  __typename
}

fragment parentContainerFields on Container {
  urn
  type
  properties {
    name
    __typename
  }
  subTypes {
    typeNames
    __typename
  }
  __typename
}

fragment deprecationFields on Deprecation {
  actor
  deprecated
  note
  decommissionTime
  actorEntity {
    urn
    type
    ...entityDisplayNameFields
    __typename
  }
  replacement {
    ...entityDisplayNameFields
    ... on Dataset {
      platform {
        ...platformFields
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment entityDisplayNameFields on Entity {
  urn
  type
  ... on Dataset {
    name
    properties {
      name
      qualifiedName
      __typename
    }
    __typename
  }
  ... on CorpUser {
    username
    properties {
      displayName
      title
      firstName
      lastName
      fullName
      email
      __typename
    }
    info {
      active
      displayName
      title
      firstName
      lastName
      fullName
      email
      __typename
    }
    __typename
  }
  ... on CorpGroup {
    name
    info {
      displayName
      __typename
    }
    __typename
  }
  ... on Dashboard {
    dashboardId
    properties {
      name
      __typename
    }
    __typename
  }
  ... on Chart {
    chartId
    properties {
      name
      __typename
    }
    __typename
  }
  ... on DataFlow {
    properties {
      name
      __typename
    }
    __typename
  }
  ... on DataJob {
    jobId
    properties {
      name
      __typename
    }
    __typename
  }
  ... on GlossaryTerm {
    name
    hierarchicalName
    properties {
      name
      __typename
    }
    __typename
  }
  ... on GlossaryNode {
    properties {
      name
      description
      __typename
    }
    __typename
  }
  ... on Domain {
    properties {
      name
      __typename
    }
    __typename
  }
  ... on Container {
    properties {
      name
      __typename
    }
    __typename
  }
  ... on MLFeatureTable {
    name
    __typename
  }
  ... on MLFeature {
    name
    __typename
  }
  ... on MLPrimaryKey {
    name
    __typename
  }
  ... on MLModel {
    name
    __typename
  }
  ... on MLModelGroup {
    name
    __typename
  }
  ... on Tag {
    name
    properties {
      name
      colorHex
      __typename
    }
    __typename
  }
  ... on DataPlatform {
    ...nonConflictingPlatformFields
    __typename
  }
  ... on DataProduct {
    properties {
      name
      __typename
    }
    __typename
  }
  ... on DataPlatformInstance {
    instanceId
    __typename
  }
  ... on StructuredPropertyEntity {
    definition {
      displayName
      qualifiedName
      __typename
    }
    __typename
  }
  ... on SchemaFieldEntity {
    fieldPath
    __typename
  }
  ... on OwnershipTypeEntity {
    info {
      name
      __typename
    }
    __typename
  }
  __typename
}

fragment nonConflictingPlatformFields on DataPlatform {
  urn
  type
  name
  properties {
    displayName
    datasetNameDelimiter
    logoUrl
    __typename
  }
  displayName
  info {
    type
    displayName
    datasetNameDelimiter
    logoUrl
    __typename
  }
  __typename
}

fragment entityHealth on Health {
  type
  status
  message
  causes
  __typename
}

fragment datasetStatsFields on Dataset {
  lastProfile: datasetProfiles(limit: 1) {
    rowCount
    columnCount
    sizeInBytes
    timestampMillis
    __typename
  }
  lastOperation: operations(limit: 1) {
    lastUpdatedTimestamp
    timestampMillis
    __typename
  }
  statsSummary {
    queryCountLast30Days
    uniqueUserCountLast30Days
    topUsersLast30Days {
      urn
      type
      username
      properties {
        displayName
        firstName
        lastName
        fullName
        __typename
      }
      editableProperties {
        displayName
        pictureLink
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment structuredPropertiesFields on StructuredPropertiesEntry {
  structuredProperty {
    exists
    ...structuredPropertyFields
    __typename
  }
  values {
    ... on StringValue {
      stringValue
      __typename
    }
    ... on NumberValue {
      numberValue
      __typename
    }
    __typename
  }
  valueEntities {
    urn
    type
    ...entityDisplayNameFields
    __typename
  }
  associatedUrn
  __typename
}

fragment structuredPropertyFields on StructuredPropertyEntity {
  urn
  type
  definition {
    displayName
    qualifiedName
    description
    cardinality
    immutable
    valueType {
      urn
      type
      info {
        type
        displayName
        __typename
      }
      __typename
    }
    entityTypes {
      urn
      type
      info {
        type
        __typename
      }
      __typename
    }
    cardinality
    typeQualifier {
      allowedTypes {
        urn
        type
        info {
          type
          displayName
          __typename
        }
        __typename
      }
      __typename
    }
    allowedValues {
      value {
        ... on StringValue {
          stringValue
          __typename
        }
        ... on NumberValue {
          numberValue
          __typename
        }
        __typename
      }
      description
      __typename
    }
    created {
      time
      actor {
        urn
        editableProperties {
          displayName
          pictureLink
          __typename
        }
        ...entityDisplayNameFields
        __typename
      }
      __typename
    }
    lastModified {
      time
      actor {
        urn
        editableProperties {
          displayName
          pictureLink
          __typename
        }
        ...entityDisplayNameFields
        __typename
      }
      __typename
    }
    __typename
  }
  settings {
    isHidden
    showInSearchFilters
    showAsAssetBadge
    showInAssetSummary
    showInColumnsTable
    __typename
  }
  __typename
}

fragment browsePathV2Fields on BrowsePathV2 {
  path {
    name
    entity {
      urn
      type
      ...entityDisplayNameFields
      __typename
    }
    __typename
  }
  __typename
}

fragment nonRecursiveDataFlowFields on DataFlow {
  urn
  type
  orchestrator
  flowId
  cluster
  properties {
    name
    description
    project
    externalUrl
    customProperties {
      key
      value
      __typename
    }
    __typename
  }
  editableProperties {
    description
    __typename
  }
  ownership {
    ...ownershipFields
    __typename
  }
  platform {
    ...platformFields
    __typename
  }
  domain {
    ...entityDomain
    __typename
  }
  ...entityDataProduct
  deprecation {
    ...deprecationFields
    __typename
  }
  __typename
}

fragment glossaryNode on GlossaryNode {
  urn
  type
  properties {
    name
    description
    __typename
  }
  displayProperties {
    ...displayPropertiesFields
    __typename
  }
  children: relationships(
    input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 1000}
  ) {
    total
    relationships {
      type
      entity {
        ... on GlossaryTerm {
          urn
          name
          type
          hierarchicalName
          properties {
            name
            description
            __typename
          }
          __typename
        }
        ... on GlossaryNode {
          urn
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment nonRecursiveMLFeature on MLFeature {
  urn
  type
  exists
  lastIngested
  name
  featureNamespace
  description
  dataType
  properties {
    description
    dataType
    version {
      versionTag
      __typename
    }
    sources {
      urn
      name
      type
      origin
      description
      uri
      platform {
        ...platformFields
        __typename
      }
      platformNativeType
      __typename
    }
    __typename
  }
  ownership {
    ...ownershipFields
    __typename
  }
  institutionalMemory {
    ...institutionalMemoryFields
    __typename
  }
  status {
    removed
    __typename
  }
  glossaryTerms {
    ...glossaryTerms
    __typename
  }
  domain {
    ...entityDomain
    __typename
  }
  ...entityDataProduct
  tags {
    ...globalTagsFields
    __typename
  }
  editableProperties {
    description
    __typename
  }
  deprecation {
    ...deprecationFields
    __typename
  }
  dataPlatformInstance {
    ...dataPlatformInstanceFields
    __typename
  }
  browsePathV2 {
    ...browsePathV2Fields
    __typename
  }
  featureTables: relationships(
    input: {types: ["Contains"], direction: INCOMING, start: 0, count: 100}
  ) {
    relationships {
      type
      entity {
        ... on MLFeatureTable {
          platform {
            ...platformFields
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
  structuredProperties {
    properties {
      ...structuredPropertiesFields
      __typename
    }
    __typename
  }
  __typename
}

fragment institutionalMemoryFields on InstitutionalMemory {
  elements {
    url
    actor {
      ...resolvedActorFields
      __typename
    }
    description
    created {
      actor
      time
      __typename
    }
    associatedUrn
    __typename
  }
  __typename
}

fragment resolvedActorFields on ResolvedActor {
  ... on CorpUser {
    urn
    ...entityDisplayNameFields
    __typename
  }
  ... on CorpGroup {
    urn
    ...entityDisplayNameFields
    __typename
  }
  __typename
}

fragment nonRecursiveMLPrimaryKey on MLPrimaryKey {
  urn
  type
  exists
  lastIngested
  name
  featureNamespace
  description
  dataType
  properties {
    description
    dataType
    version {
      versionTag
      __typename
    }
    sources {
      urn
      name
      type
      origin
      description
      uri
      platform {
        ...platformFields
        __typename
      }
      platformNativeType
      __typename
    }
    __typename
  }
  ownership {
    ...ownershipFields
    __typename
  }
  institutionalMemory {
    ...institutionalMemoryFields
    __typename
  }
  status {
    removed
    __typename
  }
  glossaryTerms {
    ...glossaryTerms
    __typename
  }
  domain {
    ...entityDomain
    __typename
  }
  ...entityDataProduct
  tags {
    ...globalTagsFields
    __typename
  }
  editableProperties {
    description
    __typename
  }
  deprecation {
    ...deprecationFields
    __typename
  }
  dataPlatformInstance {
    ...dataPlatformInstanceFields
    __typename
  }
  featureTables: relationships(
    input: {types: ["KeyedBy"], direction: INCOMING, start: 0, count: 100}
  ) {
    relationships {
      type
      entity {
        ... on MLFeatureTable {
          platform {
            ...platformFields
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
  structuredProperties {
    properties {
      ...structuredPropertiesFields
      __typename
    }
    __typename
  }
  __typename
}

fragment dataProductSearchFields on DataProduct {
  urn
  type
  properties {
    name
    description
    externalUrl
    __typename
  }
  ownership {
    ...ownershipFields
    __typename
  }
  tags {
    ...globalTagsFields
    __typename
  }
  glossaryTerms {
    ...glossaryTerms
    __typename
  }
  domain {
    ...entityDomain
    __typename
  }
  entities(input: {start: 0, count: 0, query: "*"}) {
    total
    __typename
  }
  __typename
}

fragment ermodelrelationPropertiesFields on ERModelRelationshipProperties {
  name
  source {
    ...datasetERModelRelationshipFields
    __typename
  }
  destination {
    ...datasetERModelRelationshipFields
    __typename
  }
  relationshipFieldMappings {
    ...relationshipFieldMapping
    __typename
  }
  createdTime
  createdActor {
    urn
    __typename
  }
  __typename
}

fragment datasetERModelRelationshipFields on Dataset {
  urn
  name
  properties {
    name
    description
    __typename
  }
  editableProperties {
    description
    __typename
  }
  schemaMetadata {
    ...schemaMetadataFields
    __typename
  }
  __typename
}

fragment schemaMetadataFields on SchemaMetadata {
  aspectVersion
  createdAt
  datasetUrn
  name
  platformUrn
  version
  cluster
  hash
  platformSchema {
    ... on TableSchema {
      schema
      __typename
    }
    ... on KeyValueSchema {
      keySchema
      valueSchema
      __typename
    }
    __typename
  }
  fields {
    ...entitySchemaFieldFields
    __typename
  }
  primaryKeys
  foreignKeys {
    name
    sourceFields {
      fieldPath
      __typename
    }
    foreignFields {
      fieldPath
      __typename
    }
    foreignDataset {
      urn
      name
      type
      origin
      uri
      properties {
        description
        __typename
      }
      platform {
        ...platformFields
        __typename
      }
      platformNativeType
      ownership {
        ...ownershipFields
        __typename
      }
      globalTags {
        ...globalTagsFields
        __typename
      }
      glossaryTerms {
        ...glossaryTerms
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment entitySchemaFieldFields on SchemaField {
  fieldPath
  label
  jsonPath
  nullable
  description
  type
  nativeDataType
  recursive
  isPartOfKey
  isPartitioningKey
  globalTags {
    ...globalTagsFields
    __typename
  }
  glossaryTerms {
    ...glossaryTerms
    __typename
  }
  schemaFieldEntity {
    ...entitySchemaFieldEntityFields
    __typename
  }
  __typename
}

fragment entitySchemaFieldEntityFields on SchemaFieldEntity {
  deprecation {
    ...deprecationFields
    __typename
  }
  urn
  fieldPath
  type
  structuredProperties {
    properties {
      ...structuredPropertiesFields
      __typename
    }
    __typename
  }
  businessAttributes {
    businessAttribute {
      ...businessAttribute
      __typename
    }
    __typename
  }
  documentation {
    ...documentationFields
    __typename
  }
  __typename
}

fragment businessAttribute on BusinessAttributeAssociation {
  businessAttribute {
    urn
    type
    ownership {
      ...ownershipFields
      __typename
    }
    properties {
      name
      description
      businessAttributeDataType: type
      lastModified {
        time
        __typename
      }
      created {
        time
        __typename
      }
      tags {
        tags {
          tag {
            urn
            name
            properties {
              name
              __typename
            }
            __typename
          }
          associatedUrn
          __typename
        }
        __typename
      }
      glossaryTerms {
        terms {
          term {
            urn
            type
            properties {
              name
              __typename
            }
            __typename
          }
          associatedUrn
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
  associatedUrn
  __typename
}

fragment documentationFields on Documentation {
  documentations {
    documentation
    attribution {
      time
      actor {
        urn
        type
        ...entityDisplayNameFields
        __typename
      }
      source {
        urn
        type
        __typename
      }
      sourceDetail {
        key
        value
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment relationshipFieldMapping on RelationshipFieldMapping {
  sourceField
  destinationField
  __typename
}

fragment ermodelrelationEditablePropertiesFields on ERModelRelationshipEditableProperties {
  description
  name
  __typename
}

fragment businessAttributeFields on BusinessAttribute {
  urn
  type
  ownership {
    ...ownershipFields
    __typename
  }
  properties {
    name
    description
    businessAttributeDataType: type
    customProperties {
      key
      value
      associatedUrn
      __typename
    }
    lastModified {
      time
      __typename
    }
    created {
      time
      __typename
    }
    tags {
      tags {
        tag {
          urn
          name
          properties {
            name
            __typename
          }
          __typename
        }
        associatedUrn
        __typename
      }
      __typename
    }
    glossaryTerms {
      terms {
        term {
          urn
          type
          properties {
            name
            __typename
          }
          __typename
        }
        associatedUrn
        __typename
      }
      __typename
    }
    __typename
  }
  institutionalMemory {
    ...institutionalMemoryFields
    __typename
  }
  __typename
}

fragment versionProperties on VersionProperties {
  versionSet {
    urn
    type
    __typename
  }
  isLatest
  version {
    versionTag
    __typename
  }
  aliases {
    versionTag
    __typename
  }
  comment
  created {
    time
    actor {
      urn
      ...entityDisplayNameFields
      editableProperties {
        displayName
        pictureLink
        __typename
      }
      __typename
    }
    __typename
  }
  createdInSource {
    time
    actor {
      urn
      ...entityDisplayNameFields
      editableProperties {
        displayName
        pictureLink
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment dataProcessInstanceFields on DataProcessInstance {
  urn
  type
  exists
  status {
    removed
    __typename
  }
  parentContainers {
    ...parentContainersFields
    __typename
  }
  container {
    ...entityContainer
    __typename
  }
  subTypes {
    typeNames
    __typename
  }
  properties {
    name
    created {
      time
      actor
      __typename
    }
    customProperties {
      key
      value
      __typename
    }
    externalUrl
    __typename
  }
  mlTrainingRunProperties {
    id
    outputUrls
    trainingMetrics {
      name
      description
      value
      __typename
    }
    hyperParams {
      name
      description
      value
      __typename
    }
    __typename
  }
  optionalPlatform: platform {
    ...platformFields
    __typename
  }
  dataPlatformInstance {
    ...dataPlatformInstanceFields
    __typename
  }
  state(startTimeMillis: null, endTimeMillis: null, limit: 1) {
    status
    attempt
    result {
      resultType
      nativeResultType
      __typename
    }
    timestampMillis
    durationMillis
    __typename
  }
  parentTemplate {
    urn
    type
    ... on Dataset {
      name
      properties {
        name
        description
        qualifiedName
        __typename
      }
      editableProperties {
        description
        __typename
      }
      platform {
        ...platformFields
        __typename
      }
      subTypes {
        typeNames
        __typename
      }
      status {
        removed
        __typename
      }
      __typename
    }
    ... on DataJob {
      urn
      type
      dataFlow {
        ...nonRecursiveDataFlowFields
        __typename
      }
      jobId
      properties {
        name
        description
        externalUrl
        customProperties {
          key
          value
          __typename
        }
        __typename
      }
      deprecation {
        ...deprecationFields
        __typename
      }
      dataPlatformInstance {
        ...dataPlatformInstanceFields
        __typename
      }
      subTypes {
        typeNames
        __typename
      }
      editableProperties {
        description
        __typename
      }
      status {
        removed
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment entityContainer on Container {
  urn
  platform {
    ...platformFields
    __typename
  }
  properties {
    name
    __typename
  }
  subTypes {
    typeNames
    __typename
  }
  deprecation {
    ...deprecationFields
    __typename
  }
  __typename
}

fragment entityField on SchemaFieldEntity {
  urn
  type
  parent {
    urn
    type
    ...entityDisplayNameFields
    ... on Dataset {
      platform {
        ...platformFields
        __typename
      }
      dataPlatformInstance {
        ...dataPlatformInstanceFields
        __typename
      }
      parentContainers {
        ...parentContainersFields
        __typename
      }
      __typename
    }
    __typename
  }
  fieldPath
  structuredProperties {
    properties {
      ...structuredPropertiesFields
      __typename
    }
    __typename
  }
  businessAttributes {
    businessAttribute {
      ...businessAttribute
      __typename
    }
    __typename
  }
  __typename
}
"""

# 특정 URN과 연관된 쿼리를 찾는 GraphQL 쿼리 (수정된 버전)
QUERIES_BY_URN_QUERY = """
query listQueries($input: ListQueriesInput!) {
  listQueries(input: $input) {
    start
    total
    count
    queries {
      urn
      properties {
        name
        description
        statement {
          value
          language
          __typename
        }
        __typename
      }
      subjects {
        dataset {
          urn
          name
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
}
"""

# 특정 URN의 glossary terms를 조회하는 GraphQL 쿼리
GLOSSARY_TERMS_BY_URN_QUERY = """
query getDataset($urn: String!) {
  dataset(urn: $urn) {
    urn
    name
    glossaryTerms {
      terms {
        term {
          urn
          name
          type
          hierarchicalName
          properties {
            name
            description
            definition
            __typename
          }
          parentNodes {
            nodes {
              urn
              properties {
                name
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
}
"""
