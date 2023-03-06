    SELECT 
        cps.id, 
        cpb.customer_profile_id,
        s_id, 
        cps.account_id, 
        customer_profile_behaviour_id, 
        channel, 
        utm_source, 
        utm_medium, 
        utm_campaign, 
        utm_content, 
        utm_term,
        ip, 
        ipcountry, 
        browser_family, 
        browser_version, 
        os_family, 
        os_version, 
        device_family, 
        device_brand, 
        device_model,
        device_type, 
        screen, 
        "language", 
        session_start, 
        session_end, 
        cps.created, 
        cps.updated, 
        actions_count as events_number, 
        page_views_count, 
        referer,
        add_to_basket_count,
        garbage_session,
        s_ci,
        source_id,
        medium_id,
        campaign_id,
        source_medium_id,
        CASE WHEN is_conversion IS NULL THEN False ELSE is_conversion END as is_buy_session,
        ssd.source,
        smd.medium,
        ssmd.source_medium,
        scd.campaign
    FROM "data".customer_profile_sessions cps
    LEFT OUTER JOIN data.customer_profile_behaviour cpb on cpb.id = cps.customer_profile_behaviour_id
    LEFT OUTER JOIN data.sessions_source_dict ssd on ssd.id = cps.source_id
    LEFT OUTER JOIN data.sessions_medium_dict smd on smd.id = cps.medium_id
    LEFT OUTER JOIN data.sessions_source_medium_dict ssmd on ssmd.id = cps.source_medium_id 
    LEFT OUTER JOIN data.sessions_campaign_dict scd on scd.id = cps.campaign_id
    WHERE cps.garbage_session = False and cps.session_start >= %(start_date)s and cps.session_start < %(end_date)s
    and cps.account_id = %(account_id)s;

