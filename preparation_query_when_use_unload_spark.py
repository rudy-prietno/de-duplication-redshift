class query_pararelize_services :
    query_create_table_stagging = (
        """
        create table if not exists {stagging_table} (like {destination_table});
        """
    )

    query_loadData_to_stagging = (
        """
        copy {stagging_table}
        from '{s3path}/{ingestion_date}/'
        iam_role '{iam_role}'
        parquet;
        """
    )


    query_delete_conflict_dest_stagging = (
        """
        delete from {destination_table}
        using {stagging_table}
        where {destination_table}.{destination_column_primary} = {stagging_table}.{stagging_column_primary};
        """
    )

    query_insert_dest_from_stagging = (
        """
        insert into {destination_table}
        select * from {stagging_table};
        """
    )


    query_drop_stagging = (
        """
        drop table {stagging_table} cascade;
        """
    )
