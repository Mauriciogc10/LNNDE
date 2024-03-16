####################################
########       Jobs   ##############
####################################
CREATE view vw_SAGE100_JC_Job AS
                                       SELECT 'CLT:' + LTrim(RTrim(CAST (a.JobNo AS VARCHAR (MAX)))) AS ExternalKey,
      LTrim(RTrim(CAST (a.JobNo AS VARCHAR (MAX)))) AS Name,
      T1.SFDCID AS Account__c,
      T2.SFDCID AS Sage100_Customer_Records__c,
      a.ARDivisionNo,
      a.AccountingMethod,
      a.ActualPercentComplete,
      a.ActualStartDate,
      a.AddressLine1,
      a.AddressLine2,
      a.AddressLine3,
      a.AwardedDate,
      a.AwardingContractorNo,
      a.BatchFax,
      a.BilledEstimateAmt,
      a.BillingMethod,
      a.CalculateSalesTax,
      a.CertifiedPayrollForm,
      a.City,
      a.Comment,
      a.ContactName,
      a.ContractDate,
      a.ContractNo,
      a.ContractorLicenseNo,
      a.CountryCode,
      a.CustomerNo,
      a.DateCreated,
      a.DateReported,
      a.DateUpdated,
      a.DeferredRevenue,
      a.EmailAddress,
      a.EstimatedCompDate,
      a.EstimatedStartDate,
      a.Estimator,
      a.ExcludeFromCertifiedPRReport,
      a.FaxNo,
      a.FederalStateProjectNo,
      a.JobDesc,
      a.JobNo,
      a.JobStatus,
      a.JobType,
      a.LastBillDate,
      a.LastCostTransDate,
      a.LastPaymentDate,
      a.Manager,
      a.OriginalContractAmt,
      a.OriginalEstimateAmt,
      a.PrimeContractorNo,
      a.ProjectCounty,
      a.RetainTransactionDetail,
      a.RetentionBalance,
      a.RetentionPercent,
      a.RevisedContractAmt,
      a.RevisedEstimateAmt,
      a.SortField,
      a.State,
      a.StatusDate,
      a.Subcontractor,
      a.TelephoneExt,
      a.TelephoneNo,
      a.TelephoneType,
      a.TimeCreated,
      a.TimeUpdated,
      a.TotalJobUnits,
      a.TypeOfWork,
      a.UDF_BILLING_NEXT_MO,
      a.UDF_BILLING_THIS_MO,
      a.UDF_DOCUMENT_FOLDER_URL,
      a.UDF_JOB_PMT_TERMS,
      a.UDF_ML_UDF_ACTION_ITEMS,
      a.UnbilledCost,
      a.UnitOfMeasureDesc,
      a.UserCreatedKey,
      a.UserUpdatedKey,
      a.ZipCode,
      a.UDF_BILLED_JTD,
      a.UDF_BILLED_PTD,
      a.UDF_BILLED_YTD,
      a.UDF_COSTS_JTD,
      a.UDF_COSTS_PTD,
      a.UDF_COSTS_YTD,
      a.UDF_RECEIVED_JTD,
      a.UDF_RECEIVED_PTD,
      a.UDF_RECEIVED_YTD,
      a.UDF_TAX_JTD,
      a.UDF_TAX_PTD,
      a.UDF_TAX_YTD,
      'CLT' AS Company__c,
      a.TimeStamp AS TimeStamp
FROM SF_ERP_Salesforce_Clone_JC_Job AS a
    INNER JOIN
    SF_ERP_Salesforce_Clone_AR_CUSTOMER AS C
    ON LTrim(RTrim(a.ARDivisionNo)) = LTrim(RTrim(C.ARDivisionNo))
       AND LTrim(RTrim(a.CustomerNo)) = LTrim(RTrim(C.CustomerNo))
    INNER JOIN
    TimeStampRepository AS T1
    ON T1.[Key] = 'vw_SAGE100US_Account:' + 'CLT:' + LTrim(RTrim(a.ARDivisionNo)) + ':' + LTrim(RTrim(a.CustomerNo))
    INNER JOIN
    TimeStampRepository AS T2
    ON T2.[Key] = 'vw_SAGE100US_Customer:' + 'CLT:' + LTrim(RTrim(a.ARDivisionNo)) + ':' + LTrim(RTrim(a.CustomerNo))
    LEFT OUTER JOIN
    TimeStampRepository AS TimeStampRepository
    ON 'vw_SAGE100_JC_Job:' + +'CLT:' + LTrim(RTrim(CAST (a.JobNo AS VARCHAR (MAX)))) = TimeStampRepository.[Key]
       AND a.TimeStamp = TimeStampRepository.SavedTimeStamp
WHERE TimeStampRepository.SavedTimeStamp IS NULL

####################################
########       Vendor   ##############
####################################


CREATE view vw_SAGE100US_Vendor AS
                                       SELECT DISTINCT V.TimeStamp AS TimeStamp,
               'CLT:' + 'AP:' + LTrim(RTrim(V.VendorNo)) AS CommercientSF__Commercient_ArCustomerCode__c,
               IsNull(V.VendorName, V.VendorNo) AS NAME,
               V.TelephoneNo AS Phone,
               V.UDF_DOCUMENT_FOLDER_URL AS Document_Folder_URL__c,
               V.EmailAddress AS Email__c,
               'CLT' AS Company__c,
               LTrim(RTrim(V.APDivisionNo)) AS AP_Division__c,
               IsNull(V.Comment, '') AS Comments__c,
               LTrim(RTrim(V.VendorNo)) AS Customer_Number__c,
               IsNull(V.FaxNo, '') AS Fax,
               IsNull(V.TelephoneExt, '') AS Phone_Extension__c,
               IsNull(V.URLAddress, '') AS Website,
               '0054x000006dUOTAA2' AS OwnerId,
               IsNull(V.AddressLine1, '') AS Street_Address_1__c,
               IsNull(V.AddressLine2, '') AS Street_Address_2__c,
               IsNull(V.AddressLine3, '') AS Street_Address_3__c,
               SubString(V.City, 0, 30) AS City__c,
               LTrim(RTrim(IsNull(V.State, ''))) AS State__c,
               V.ZipCode AS Postal_Code__c,
               V.CountryCode AS Country__c,
               '0124x000000q1pLAAQ' AS RecordTypeId,
               TermsCode AS AP_Terms_Code__c
FROM SF_ERP_Salesforce_Clone_AP_VENDOR AS V WITH (NOLOCK)
    LEFT OUTER JOIN
    TimeStampRepository AS TS
    ON TS.[Key] = 'vw_SAGE100US_Account:' + 'CLT:' + 'AP:' + LTrim(RTrim(V.VendorNo))
       AND V.TimeStamp = TS.SavedTimeStamp
WHERE TS.SavedTimeStamp IS NULL
     AND V.VendorNo IN ('ROSCO01', 'MATTH01', 'VITEC02', 'ARRI01', 'ALTMA01', 'ALTMA31', 'ACORN30', 'PHILI04', 'OSRAM01', 'MOLER01')  

####query#####

 Name, Phone, Document_Folder_URL__c, Email__c, Company__c, AP_Division__c, Comments__c, Customer_Number__c,
Fax, Phone_Extension__c,Website,OwnerId,Street_Address_1__c, Street_Address_2__c, Street_Address_3__c, City__c, State__c,Postal_Code__c,Country__c,
 RecordTypeId, AP_Terms_Code__c