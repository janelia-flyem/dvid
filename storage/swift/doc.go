/*
Package swift adds Openstack Swift support to DVID. Mandatory configuration
parameters are:

  - user: The Swift user.
  - key: The Swift key.
  - auth: The authorization URL.
  - container: The name of the container where the data is stored. If such a
    container does not exist, it is created.
*/
package swift
