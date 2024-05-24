#FROM public.ecr.aws/lambda/python:3.7
#FROM public.ecr.aws/lambda/python:3.11-arm64
FROM public.ecr.aws/lambda/python:3.10-arm64

#Allow statements and logs messages to appear
ENV PYTHONUNBUFFERED True

# Install the function's dependencies using file requirements.txt
# from your project folder.
RUN pip3 install --upgrade pip
RUN pip3 install tensorflow
#COPY requirements.txt . 
#RUN pip3 install -r requirements.txt --target ${LAMBDA_TASK_ROOT} -U --no-cache-dir --default-timeout=1000


# Copy project code
COPY ./ ${LAMBDA_TASK_ROOT}

# Install codebase
#RUN pip3 install --upgrade pip
# RUN pip3 install -e codebase --target ${LAMBDA_TASK_ROOT} --no-cache-dir --default-timeout=1000

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]