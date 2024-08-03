{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="id",
    alias="sf_party_t",
    tags=["salesforce"]
) }}

{%- set unique_key = config.get('unique_key') -%}
{%- set incr_key = 'lastmodifieddate' -%}
{%- set column_list = 'id,
    active__c,
    address__c,
    appointment_date__c,
    bri_status__c,
    bri__c,
    bankrupcy__c,
    cbs_reuse__c,
    cbs_scanned_copy__c,
    cbs_status__c,
    common_share_holder_check__c,
    date_of_birth__c,
    entity_classification__c,
    entity_number__c,
    isdeleted,
    is_director__c,
    is_shareholder__c,
    lastmodifiedbyid,
    lastmodifieddate,
    lead_uen__c,
    ligitation__c,
    mortgage_miss__c,
    name__c,
    name,
    ownerid,
    party_removed__c,
    pending_credit_suits__c,
    pending_suit_amount__c,
    post_tax_income__c,
    pre_tax_income__c,
    relationship_to_keyman__c,
    relationship_with_subject_applicant__c,
    residential_property_ownership__c,
    risk_grade__c,
    share_holder_percentage__c,
    share_holders__c,
    sub_product__c,
    totally_monthly_installment__c,
    winups_check__c,
    year_of_assessment__c,
    clcommon__account__c,
    clcommon__contact__c,
    clcommon__electronic_consent__c,
    clcommon__joint_consent__c,
    clcommon__party_type__c,
    clcommon__signer_capacity__c,
    clcommon__signing_on_behalf_of__c,
    clcommon__type__c,
    clcommon__user__c,
    clcommon__isprimary__c,
    genesis__application__c,
    genesis__credit_check_consent__c,
    genesis__credit_report__c,
    genesis__is_internal_user__c,
    genesis__party_name__c,
    isguarantor__c,
    createddate' -%}

{% set src_table = "source." ~ config.get("alias")[:-2] %}
{% set src_data = config.get("alias").split("_")[0] %}
{{ gen_sql(unique_key, incr_key, column_list, src_data, src_table) | trim }}
