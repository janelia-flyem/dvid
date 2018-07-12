/*
Package swift adds Openstack Swift support to DVID. Mandatory configuration
parameters are:

  - user: The Swift user.
  - key: The Swift key / password.
  - auth: The authorization URL.
  - container: The name of the container where the data is stored. If such a
    container does not exist, it is created.

Optional parameters are:

  - project: The project (v3 authorization only).
  - domain: The project domain (v3 authorization only).

*/
package swift
