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
        scd.campaign,
        cpe.is_name_action_type,
        cpe.documents_mouse_out_count,
        cpe.documents_mouse_enter_count
    FROM "data".customer_profile_sessions cps
    LEFT OUTER JOIN data.customer_profile_behaviour cpb on cpb.id = cps.customer_profile_behaviour_id
    LEFT OUTER JOIN data.sessions_source_dict ssd on ssd.id = cps.source_id
    LEFT OUTER JOIN data.sessions_medium_dict smd on smd.id = cps.medium_id
    LEFT OUTER JOIN data.sessions_source_medium_dict ssmd on ssmd.id = cps.source_medium_id 
    LEFT OUTER JOIN data.sessions_campaign_dict scd on scd.id = cps.campaign_id
    inner join (
            select 
            session_id,guest_id,
            max(case
                when cpa."name" = 'link_click' or cpa."name" = 'button_click' or cpa."name" like 'scroll' then 1 
                else 0
            end
            ) as is_name_action_type,
            count(case when cpa."name" = 'document_mouse_out' then 1 else null end) as documents_mouse_out_count,
            count(case when cpa."name" = 'document_mouse_enter' then 1 else null end) as documents_mouse_enter_count
            from data.customer_profile_actions cpa 
            where account_id = %(account_id)s and cpa.event_time >= %(start_date)s and cpa.event_time < %(end_date)s
            group by cpa.session_id	, cpa.guest_id	
    ) cpe on cps.id = cpe.session_id
    WHERE cps.garbage_session = False and cps.session_start >= %(start_date)s and cps.session_start < %(end_date)s
    and cps.account_id = %(account_id)s;

