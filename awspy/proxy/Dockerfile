# Use an official Nginx runtime as a parent image
FROM nginx:latest

ENV NGINX_ENVSUBST_OUTPUT_DIR /etc/nginx

COPY nginx.conf.template /etc/nginx/templates/nginx.conf.template

EXPOSE 80

# Start Nginx when the container starts
CMD ["nginx-debug", "-g", "daemon off;"]