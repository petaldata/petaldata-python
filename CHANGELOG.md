# PetalData Changelog

## 1.0.2

* Better authentication error handling. Previously, if an auth error occurred an empty dataframe would be created and no error message would be displayed. Now, an `AuthError` exception is raised with additional details (like an invalid API key).

## 1.0.1

* Adding missing `pygsheets` dependency.

## 1.0.0

* 🚀