FROM public.ecr.aws/lambda/python:3.8
COPY ./ ${LAMBDA_TASK_ROOT}/
# Install necessary dependencies.
# Try to minimize dependencies as much as possible
# for better API response times.
RUN python3 -m pip install -r requirements.txt --target ${LAMBDA_TASK_ROOT}
# Below should reference the file api_endpoints.py
# in the same directory.
CMD ["api_endpoints.handler"]