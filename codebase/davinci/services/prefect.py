from prefect.client import get_client
import asyncio

def limit_concurrency(tag, limit):
    """
    Create a Prefect concurrency limit to impose
    on tasks that have a specific tag. 

    :param tag: the tag to mofidy
    :type tag: str
    :param limit: concurrency limit
    :type limit: int

    .. warning::
        Will change settings in the Prefect server,
        not locally. Therefore, it will be best practice
        to make your tag specific to your script. I.e.,
        don't use generic names like "SQL" or "data_process",
        use "SQL_PPLH_INGEST_DIST_CT".

    Example usage:

        .. code-block:: python

            from davinci.services.prefect import limit_concurrency

            limit_concurrency("my_very_specific_concurrent_task", 10)            

    """
    async def update_client():
        async with get_client() as client:
            limit_id = await client.create_concurrency_limit(
                tag=tag, 
                concurrency_limit=limit
                )

    asyncio.run(update_client())