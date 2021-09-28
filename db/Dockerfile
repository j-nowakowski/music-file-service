# @source: https://medium.com/@pybrarian/mysql-databases-that-dont-retain-data-293bc2ed7f02
FROM mysql:5
ENV MYSQL_ROOT_PASSWORD=root
RUN mkdir /var/lib/mysql-no-volume
CMD ["--datadir", "/var/lib/mysql-no-volume","--max_allowed_packet","1G"]
