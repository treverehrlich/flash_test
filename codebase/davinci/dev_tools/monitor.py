import pandas as pd
from datetime import datetime
import croniter

from davinci.utils.utils import _full_stack
from davinci.utils.logging import logger
from davinci.utils.global_config import PROD
from davinci.services.sql import write_df_to_table

_HEADERS = {
    'model': [
        'Date',
        'Customer',
        'Site',
        'ModelName',
        'MLType',
        'ModelType',
        'Metric',
        'MetricVal',
        'MinAccept',
        'MaxAccept',
        'Ok',
        'Env',
        'Meta1',
        'Meta2',
        'Meta3'
        ],
    'cron': [
        'Ran',
        'Customer',
        'Site',
        'JobName',
        'JobFreq',
        'NextDue',
        'Ok',
        'Notes',
        'Error'
    ],
    'data_val': [
        'dt',
        'customer',
        'check_name',
        'check_freq',
        'next_due',
        'ok'
    ]
}

_TABLES = {
    'model': 'modelMonitor',
    'cron': 'cronMonitor'
}

class _Monitor:
    def __init__(self, kind, customer, site, name):
        self.customer = customer
        self.name = name
        self.kind = kind
        self.site = site
        inputs = [kind, customer, name, site]
        if not all(map(lambda x: type(x) == str, inputs)):
            logger.info('WARNING: Monitor inputs must be str')
            return None

        self.kind = self.kind.lower()
        if self.kind not in set(_HEADERS.keys()):
            logger.info(f'WARNING: {self.kind} if not a valid Monitor type.')
            logger.info(f'Valid options are {list(_HEADERS.keys())}')
            return None
        self.columns = _HEADERS[kind]
        self.stacktrace = ""
        self.dt = datetime.now()
        self.output = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if traceback:
            self.stacktrace = _full_stack()
        try:
            self._build_data()
            write_df_to_table(self.output, _TABLES[self.kind], db='INFO_DATABASE')
        except Exception as e:
            logger.info(f'Monitor ({self.kind}) failed to write...')

class CronMonitor(_Monitor):
    """
    Add Cronjob monitor. This will aid in detecting when a
    particular piece of code has failed.

    :param customer: Customer for script.
    :type customer: str
    :param site: Site for script.
    :type site: str
    :param job_name: Job name for recording
    :type job_name: str
    :param job_freq: Cron job string. E.g., '15 17 * * Tue'.
    :type job_freq: str

    Example usage:

    .. code-block:: python

        if __name__ == "__main__":
            with CronMonitor("GMI", 'Social Circle', 'FTE Estimation', "15 17 * * *") as m:
                main(args)
                m.add_note('Detected 5 data points for prediction')
                m.ok()

    """
    def __init__(self, customer, site, job_name, job_freq):
        inputs = [customer, site, job_name, job_freq]
        super().__init__('cron', customer, site, job_name)
        if not all(map(lambda x: type(x) == str, inputs)):
            logger.info('WARNING: Monitor inputs must be str')
            return None
        self.job_name = job_name
        self.job_freq = job_freq
        self.note = None
        self.is_ok = False
        try:
            self.next_due = croniter.croniter(job_freq, self.dt).get_next(datetime)
        except croniter.CroniterBadCronError:
            self.next_due = 'BAD SCHEDULE'

    def add_note(self, s):
        """
        Add a note with the job record in database.
        Only one note can be added; if multiple are
        added, only the last will be recorded.

        :param s: Custom note
        :type s: str

        """
        self.note =  s

    def ok(self):
        """
        Confirm that the code has executed.
        This should usually be placed as the very
        last step in code. See example.
        """
        self.is_ok = True
        return True

    def _build_data(self):
        new_data = pd.DataFrame(
                [[self.dt, self.customer, self.site, self.job_name, self.job_freq, self.next_due, self.is_ok, self.note, self.stacktrace]],
                columns=self.columns
            )
        self.output = new_data
        
class ModelMonitor(_Monitor):
    """
    Add Model monitor. This will aid in tracking
    model performance as we retrain models.

    :param customer: Customer for model.
    :type customer: str
    :param site: Site for model.
    :type site: str
    :param model_name: Name of the model
    :type model_name: str
    :param model_type: e.g., 'Volume Prediction'
    :type model_type: str
    :param model_type: e.g., 'Regressor'
    :type model_type: str

    Example usage:

    .. code-block:: python

        # In general, place the ModelMonitor right after 
        # a model gets saved into a production location (like S3).
        # This way, if the code crashes somewhere after, we still
        # have the model for prediction uses and the monitor record together.
        with ModelMonitor('GMI', 'Social Circle', 'IBPallets', 'FTE Estimator', 'Regressor') as m:
            m.track('RMSE', rmse_val, 1, 10)
            m.track('R2', r2_val, 0.50, 0.95)
            m.meta('PyCaret Blended Top 3')
            m.meta('RMSE in hours')

    """
    def __init__(self, customer, site, model_name, model_type, ml_type):
        inputs = [customer, site, model_name, model_type, ml_type]
        super().__init__('model', customer, site, model_name)
        if not all(map(lambda x: type(x) == str, inputs)):
            logger.info('WARNING: Monitor inputs must be str')
            return None
        self.model_name = model_name
        self.model_type = model_type
        self.ml_type = ml_type
        self.rows = []
        self.meta_data = []

    def meta(self, s):
        """
        Add meta data with record. Only
        three calls to meta() will be registered.

        :param s: meta data string
        :type s: str
        """
        if len(self.meta_data) <  3:
            self.meta_data.append(s)
        else:
            logger.warning('Warning: can only add 3 meta data entries')

    def track(self, metric, val, lb, ub):
        """
        Track a model metric. Add as many
        calls to track() as needed.

        :param metric: Which metric are you tracking
        :type metric: str
        :param val: Metric value
        :type val: float
        :param lb: Acceptable lower bound of metric
        :type lb: float
        :param ub: Acceptable upper bound of metric
        :type ub: float
        """
        is_ok = lb <= val <= ub
        row = [self.dt, self.customer, self.site, self.model_name, self.ml_type, self.model_type, metric, val, lb, ub, is_ok]
        self.rows.append(row)

    def _build_data(self):
        new_data = pd.DataFrame(self.rows)
        new_data['env'] = 'PROD' if PROD else 'DEV'
        new_data[[f'meta_{i}' for i in range(1, 4)]] = ""
        for i, meta in enumerate(self.meta_data):
            new_data[f'meta_{i + 1}'] = meta
        new_data.columns = self.columns
        self.output = new_data
        self.output.columns = self.columns