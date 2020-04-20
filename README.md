# Microsoft Dynamics 365 Extractor

[TOC]

## Introduction

Microsoft Dynamics 365 is a product line of customer relationship management by Microsoft. The Microsoft Dynamics 365 extractor for Keboola allows users to extract data from their Dynamics instance. The extractor supports downloading data from any available entity in Dynamics 365 instance, including custom entities.

The extractor utilizes WebAPI Graph API and supports all versions of the API. For more information about the WebAPI, please read [the API reference](https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/about).

## Configuration

A sample configuration of the writer can be found in [component's repository](https://bitbucket.org/kds_consulting_team/kds-team.wr-microsoft-dynamics/src/master/component_config/sample-config/). 

### Authorization

The component needs to be authorized by a user with access to Dynamics 365. The writer then performs all of the operations on behalf of the user, i.e. all of the operations have user's unique identification linked to the operation.

For local run of the writer, please refer to [correct configuration file specification](https://developers.keboola.com/extend/common-interface/oauth/#authorize).

### Parameters

#### Organization URL (`organization_url`)

The URL of Dynamics 365 instance, where all API calls will be made. The URL can be discovered using [Global Discovery Service](https://docs.microsoft.com/en-us/powerapps/developer/common-data-service/webapi/discover-url-organization-web-api) or from the URL of web instance:

![organization_url](docs/images/organization_url.png)


#### API Version (`api_version`)

The API version of WebAPI which will be used to query the data. For a list of available APIs, please visit [API reference](https://docs.microsoft.com/en-us/dynamics365/customerengagement/on-premises/developer/webapi/web-api-versions).

#### Endpoint (`endpoint`)

For a list of default Microsoft defined entities, please visit [Web API EntityType Reference](https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/entitytypes). This list, however, does not include custom created entities and fields. For a complete list of entities, visit `[ORGANIZATION_URL]/api/data/[API_VERSION]/EntityDefinitions?%24select=EntitySetName`, where `ORGANIZATION_URL` is a unique URL of the Dynamics CRM instance for your organization, and `API_VERSION` is the API version specification, you'd like to use; e.g. `https://keboola.crm.dynamics.com/api/data/v9.1/EntityDefinitions?%24select=EntitySetName`. 
*Note: If the above request returns page error (i.e. HTTP ERROR 401), you need to be logged in first to access the resouce.*

If application receives an endpoint (entity), which is not part of the CRM instance, the run will be terminated.

#### Query (`query`)

A WebAPI query allows users to further specify results which should be retrieved from the Microsoft Dynamics 365 instance. A basic tutorial on how to query data can be found in the [WebAPI documentation](https://docs.microsoft.com/en-us/powerapps/developer/common-data-service/webapi/query-data-web-api). Individual pieces of the query need to be separated by a new line or by ampersand (&). Below are discussed some examples.

For a complete list of query functions that can be used in the query, refer to [Query Function reference](https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/queryfunctions). Please note, that all keywords must start with `$`.

##### Selecting relevant columns

Using keyword `$select`, it's possible to only specify columns which should be returned from the API. The list of columms should be comma-separated. If nothing is specified, all columns are returned.
*Please note, that all **columns which start with `fk`** in the output table from the extractor, **do not contain the `fk` prefix** in the API. Thus, if you'd like to query a column starting with `fk`, **omit the prefix**; e.g. in the output table, a column named `fk_primarycontactid_value` would be queried using `$select=_primarycontactid_value`.*

An example of `$select` query would be:

```
$select=firstname,lastname,emailaddress1,_accountid_value
```

##### Downloading a sample of the data

Using keyword `$top`, you can limit the number of results returned by the API. This is especially useful, if you're unaware what attributes are returned by the API and want to avoid lengthy downloads of all records and all attributes. The `$top` keyword will only returned first X specified records.

An example of `$top` query would be:

```
$top=5000
```

##### Filtering results

Using keyword `$filter`, it's possible to only filter results, which satisy all conditions specified. WebAPI offers a wide range of functions, which allow to filter results to the utmost detail.

**Time-based filtering**

Each record in the Dynamics CRM contains two metadata columns `createdon` and `modifiedon`, which can be used query data with latest changes. Upon creation of each record, `createdon` and `modifiedon` share the same value; once a record is modified, `modifiedon` takes on a new value while `createdon` does not change. It is therefore recommended to query on field `modifiedon`, to capture all of the changes in records, including newly created records.

An example of time-based filtering could be:

```
-- download data for past 30 days
$filter=Microsoft.Dynamics.CRM.LastXDays(PropertyName='modifiedon',PropertyValue=30)

-- download data with modified date greater than
$filter=modifiedon gt 2020-01-01
```

**Attrribute-based filtering**

Attribute-based filtering allows to filter data based on values of attributes of each records. 

An example of attribute-based filtering could be:
```
-- download data which have 'test' in emailaddress1
$filter=contains(emailaddress1,'test')

-- download data which start with 'Name' in property firstname
$filter=startswith(firstname,'Name')
```

##### Complete queries

Complete queries need to be new-line separated or "&" separated. Examples of complete queries are below combining both `$select` and `$filter` are below:

**Select and filter in last 7 days**

```
$select=leadid,firstname
$filter=Microsoft.Dynamics.CRM.LastXDays(PropertyName='modifiedon',PropertyValue=7)
```

**Select and attribute and time-based chained filter**

```
$select=leadid,firstname
$filter=Microsoft.Dynamics.CRM.LastXDays(PropertyName='modifiedon',PropertyValue=7) and contains(firstname, 'Name')
```

**Ampersand delimited query**

```
$filter=modifiedon gt 2019-01-01&select=emailaddress1
```

**Select and limit**

```
$select=contactid,fullname,emailaddress1,address1_city
$top=500
```

## Output

Extractor outputs a table with desired results from an endpoint, with the same primary key that is present in the Dynamics CRM instance. The table is loaded incrementally.
Please note, that metadata columns, which are returned from the API are omitted from the result and column, which start with `fk`, have this suffix artificially added due to compatibility with Keboola Storage.

## Useful links

- [WebAPI Reference](https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/about)
- [Query Data Using WebAPI](https://docs.microsoft.com/en-us/powerapps/developer/common-data-service/webapi/query-data-web-api)
- [Use WebAPI functions](https://docs.microsoft.com/en-us/powerapps/developer/common-data-service/webapi/use-web-api-functions)
- [WebAPI Query Function Reference](https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/queryfunctions)
- [LastXDays Function](https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/lastxdays)

## Local development

For local development, use following commands to build and run an image:

```
docker-compose build dev
docker-compose run --rm dev
```