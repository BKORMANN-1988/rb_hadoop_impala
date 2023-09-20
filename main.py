from rb_hadoop_connectivity import hadoop_connectivity

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    with hadoop_connectivity.connection("rt-iplb-rbap.de.bosch.com",
                                        21050, "RB-AE-RTP2.BDPS.BOSCH-ORG.COM") as connection:
        query_base = f"""
        select 
        COALESCE(location_result_uid,"") AS location_result_uid,
        COALESCE(uniquepart_id,"") AS uniquepart_id,
        COALESCE(location_id,"") AS location_id,
        COALESCE(line_number,"") AS line_number,
        unix_timestamp(result_date) AS result_date,
        COALESCE(pstatinterval,0.0) AS pstatinterval,
        COALESCE(workcycle_counter,0.0) AS workcycle_counter,
        COALESCE(result_state,0.0) AS result_state,
        COALESCE(part_attribute,0.0) AS part_attribute,
        COALESCE(type_number,"") AS type_number,
        COALESCE(type_variant,"") AS type_variant,
        COALESCE(type_version,"") AS type_version,
        COALESCE(machine_id,"") AS machine_id,
        COALESCE(shift_code,0.0) AS shift_code,
        COALESCE(archive_flag,0.0) AS archive_flag,
        COALESCE(batch,"") AS batch,
        COALESCE(delete_date,"") AS delete_date, 
        COALESCE(test_type,0.0) AS test_type,
        COALESCE(time_stamp,"") AS time_stamp,
        COALESCE(alias,"") AS alias,
        COALESCE(result,"") AS result,
        unix_timestamp(result_date_from_results) AS result_date_from_results,
        COALESCE(param_name,"") AS param_name,
        COALESCE(location_detail,"") AS location_detail,
        COALESCE(data_type,0.0) AS data_type,
        COALESCE(result_string,"") AS result_string,
        COALESCE(result_value,"") AS result_value,
        COALESCE(result_state_from_results,0.0) AS result_state_from_results,
        COALESCE(unit,"") AS unit,
        COALESCE(paa_rel,"") AS paa_rel,
        COALESCE(tolerance_type,0.0) AS tolerance_type,
        COALESCE(lower_tolerance,"0") AS lower_tolerance,
        COALESCE(upper_tolerance,"0") AS upper_tolerance,
        COALESCE(set_value,"") AS set_value,
        COALESCE(set_string,"") AS set_string,
        COALESCE(result_type,"") AS result_type,
        COALESCE(repository,"") AS repository,
        COALESCE(time_stamp_from_results,"") AS time_stamp_from_results,
        COALESCE(loc_validation_code,"") AS loc_validation_code,
        COALESCE(res_validation_code,"") AS res_validation_code,
        COALESCE(process_number,"") AS process_number,
        COALESCE(process_date,"") AS process_date 
        from
        (
            select
                substr(location_id, 13, 4) as station,
                substr(location_id, 17, 4) as station_index,
                substr(location_id, 21, 1) as function_unit,
                substr(location_id, 22, 4) as working_position,
                substr(location_id, 26, 4) as tool_position,
                base.*
            from proc_rtp2ap1.joined_unpivoted base
            where line_number in ('0146','0147')
              and substr(location_id, 13, 4) in ('0050','0300')
              and process_number is not null
            order by line_number, location_id, type_number, process_number, param_name, 
            result_date_from_results desc
        ) base_unpivoted
        where process_number = "1770" 
        and result_date >= to_timestamp('2023/08/01', 'yyyy/MM/dd')
        order by cast(process_number AS BIGINT), param_name
        """
