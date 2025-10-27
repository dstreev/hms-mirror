#!/usr/bin/env bash

# Copyright (c) 2024-2025. Cloudera, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

################################################################################
# HMS-Mirror Hive JDBC Driver Download Script
#
# This script downloads Hive JDBC drivers from Maven Central (and other
# repositories) to the user's local driver directory for use with HMS-Mirror.
#
# Default installation directory: $HOME/.hms-mirror/drivers
#
# Directory structure matches DriverType paths:
#   - apache/hive/1    - Apache Hive 1.x (Hive 1.2.x for HDP2, CDH5, etc.)
#   - apache/hive/2    - Apache Hive 2.x (Hive 2.x for CDH6, HDP2, etc.)
#   - apache/hive/3    - Apache Hive 3.x (Hive 3.1.3)
#   - apache/hive/4    - Apache Hive 4.0.x
#   - apache/hive/4_1  - Apache Hive 4.1.x
#   - cdh/6            - Cloudera CDH 6 (Hive 2.1.1-cdh6.3.4)
#   - hdp/2            - Hortonworks HDP 2 (Hive 1.2.1000.2.6.5.0-292)
#   - cdp/7_1_9        - Cloudera CDP 7.1.9 (Hive 3.1.3000.7.1.9.1059-4)
#   - cdp/7_3_1        - Cloudera CDP 7.3.1 (Hive 3.1.3000.7.3.1.404-1)
#
# Usage:
#   ./download-hive-drivers.sh [OPTIONS]
#
# Options:
#   -d, --dir DIR          Installation directory (default: $HOME/.hms-mirror/drivers)
#   -a, --all              Download all drivers (default)
#   -s, --select TYPES     Download specific driver types (comma-separated)
#                          Available: apache3,apache4,apache4_1,cdh6,hdp2,cdp7_1_9,cdp7_3_1,all
#   -h, --help             Show this help message
#
# Examples:
#   # Download all drivers to default location
#   ./download-hive-drivers.sh
#
#   # Download to custom location
#   ./download-hive-drivers.sh -d /opt/hms-mirror/drivers
#
#   # Download only Apache Hive 3 and 4 drivers
#   ./download-hive-drivers.sh -s apache3,apache4
#
################################################################################

set -e  # Exit on error

# Default values
DRIVER_BASE_DIR="${HOME}/.hms-mirror/drivers"
DOWNLOAD_ALL=true
SELECTED_TYPES=""

# Maven repository URLs
MAVEN_CENTRAL="https://repo1.maven.org/maven2"
CLOUDERA_REPO="https://repository.cloudera.com/artifactory/cloudera-repos"
HDP_REPO="https://repo.hortonworks.com/content/repositories/releases"

# Driver versions (should match pom.xml)
APACHE_1_VERSION="1.2.2"
APACHE_2_VERSION="2.3.10"
APACHE_3_VERSION="3.1.3"
APACHE_4_VERSION="4.0.1"
APACHE_4_1_VERSION="4.1.0"
CDH5_HIVE_VERSION="1.1.0-cdh5.16.99"
CDH5_HADOOP_VERSION="2.6.0-cdh5.16.99"
CDH6_HIVE_VERSION="2.1.1-cdh6.3.4"
CDH6_HADOOP_VERSION="3.0.0-cdh6.3.4"
HDP2_VERSION="1.2.1000.2.6.5.0-292"
CDP_7_1_9_VERSION="3.1.3000.7.1.9.1059-4"
CDP_7_3_1_VERSION="3.1.3000.7.3.1.404-1"

# Hadoop versions matching each Hive version
HADOOP_1_VERSION="1.2.1"           # For Apache Hive 1.2.2
HADOOP_2_VERSION="2.10.2"           # For Apache Hive 2.x
HADOOP_3_VERSION="3.3.6"           # For Apache Hive 3.1.3
HADOOP_4_VERSION="3.4.1"           # For Apache Hive 4.0.1
HADOOP_4_1_VERSION="3.4.1"         # For Apache Hive 4.1.0
CDP_7_1_9_HADOOP="3.1.1.7.1.9.1059-4"    # For CDP 7.1.9
CDP_7_3_1_HADOOP="3.1.1.7.3.1.404-1"     # For CDP 7.3.1

# Log4j versions
LOG4J_2_VERSION="2.18.0"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

################################################################################
# Helper Functions
################################################################################

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_help() {
    cat << EOF
HMS-Mirror Hive JDBC Driver Download Script

Usage: $(basename "$0") [OPTIONS]

Options:
  -d, --dir DIR          Installation directory (default: \$HOME/.hms-mirror/drivers)
  -a, --all              Download all drivers (default)
  -s, --select TYPES     Download specific driver types (comma-separated)
                         Available: apache3,apache4,apache4_1,cdh6,hdp2,cdp7_1_9,cdp7_3_1,all
  -h, --help             Show this help message

Examples:
  # Download all drivers to default location
  $(basename "$0")

  # Download to custom location
  $(basename "$0") -d /opt/hms-mirror/drivers

  # Download only Apache Hive 3 and 4 drivers
  $(basename "$0") -s apache3,apache4

Driver Types:
  apache3    - Apache Hive 3.1.3
  apache4    - Apache Hive 4.0.1
  apache4_1  - Apache Hive 4.1.0
  cdh6       - Cloudera CDH 6 (Hive 2.1.1-cdh6.3.4)
  hdp2       - Hortonworks HDP 2 (Hive 1.2.1000.2.6.5.0-292)
  cdp7_1_9   - Cloudera CDP 7.1.9 (Hive 3.1.3000.7.1.9.1059-4)
  cdp7_3_1   - Cloudera CDP 7.3.1 (Hive 3.1.3000.7.3.1.404-1)

EOF
}

################################################################################
# Download Functions
################################################################################

download_hadoop_dependencies() {
    local hadoop_version="$1"
    local dest_dir="$2"
    local repo_url="$3"
    local description="$4"

    print_info "Downloading Hadoop ${hadoop_version} dependencies for ${description}..."

    local hadoop_group_path="org/apache/hadoop"

    # Download hadoop-common
    local hadoop_common_jar="hadoop-common-${hadoop_version}.jar"
    local hadoop_common_url="${repo_url}/${hadoop_group_path}/hadoop-common/${hadoop_version}/${hadoop_common_jar}"
    download_jar "$hadoop_common_url" "$dest_dir" "$hadoop_common_jar" "Hadoop Common ${hadoop_version}" || true

    # Download hadoop-auth
    local hadoop_auth_jar="hadoop-auth-${hadoop_version}.jar"
    local hadoop_auth_url="${repo_url}/${hadoop_group_path}/hadoop-auth/${hadoop_version}/${hadoop_auth_jar}"
    download_jar "$hadoop_auth_url" "$dest_dir" "$hadoop_auth_jar" "Hadoop Auth ${hadoop_version}" || true
}

download_log4j_dependencies() {
    local dest_dir="$1"
    local description="$2"

    print_info "Downloading Log4j dependencies for ${description}..."

    local log4j_group_path="org/apache/logging/log4j"

    # Download log4j-api
    local log4j_api_jar="log4j-api-${LOG4J_2_VERSION}.jar"
    local log4j_api_url="${MAVEN_CENTRAL}/${log4j_group_path}/log4j-api/${LOG4J_2_VERSION}/${log4j_api_jar}"
    download_jar "$log4j_api_url" "$dest_dir" "$log4j_api_jar" "Log4j API ${LOG4J_2_VERSION}" || true

    # Download log4j-core
    local log4j_core_jar="log4j-core-${LOG4J_2_VERSION}.jar"
    local log4j_core_url="${MAVEN_CENTRAL}/${log4j_group_path}/log4j-core/${LOG4J_2_VERSION}/${log4j_core_jar}"
    download_jar "$log4j_core_url" "$dest_dir" "$log4j_core_jar" "Log4j Core ${LOG4J_2_VERSION}" || true

    # Download log4j-1.2 API Bridge
    local log4j_1_2_api_jar="log4j-1.2-api-${LOG4J_2_VERSION}.jar"
    local log4j_1_2_api_url="${MAVEN_CENTRAL}/${log4j_group_path}/log4j-1.2-api/${LOG4J_2_VERSION}/${log4j_1_2_api_jar}"
    download_jar "$log4j_1_2_api_url" "$dest_dir" "$log4j_1_2_api_jar" "Log4j 1.2 API Bridge ${LOG4J_2_VERSION}" || true

}

download_jar() {
    local url="$1"
    local output_dir="$2"
    local filename="$3"
    local description="$4"

    # Check if file already exists
    if [ -f "${output_dir}/${filename}" ]; then
        print_info "$description already exists, skipping download"
        return 0
    fi

    print_info "Downloading $description..."
    print_info "  URL: $url"
    print_info "  Destination: $output_dir/$filename"

    # Create output directory if it doesn't exist
    mkdir -p "$output_dir"

    # Download using curl with progress bar
    if curl -f -L --progress-bar -o "${output_dir}/${filename}" "$url"; then
        print_success "Downloaded $description"
        return 0
    else
        print_error "Failed to download $description"
        return 1
    fi
}

#download_apache_hive_1() {
#    local group_path="org/apache/hive/hive-jdbc"
#    local artifact="hive-jdbc-${APACHE_1_VERSION}-standalone.jar"
#    local url="${MAVEN_CENTRAL}/${group_path}/${APACHE_1_VERSION}/${artifact}"
#    local dest_dir="${DRIVER_BASE_DIR}/apache/hive/1"
#
#    download_jar "$url" "$dest_dir" "$artifact" "Apache Hive 1"
#    download_hadoop_dependencies "$HADOOP_1_VERSION" "$dest_dir" "$MAVEN_CENTRAL" "Apache Hadoop for Apache Hive 1"
#    download_log4j_dependencies "$dest_dir" "Apache Hive 1"
#}

download_apache_hive_2() {
    local group_path="org/apache/hive/hive-jdbc"
    local artifact="hive-jdbc-${APACHE_2_VERSION}-standalone.jar"
    local url="${MAVEN_CENTRAL}/${group_path}/${APACHE_2_VERSION}/${artifact}"
    local dest_dir="${DRIVER_BASE_DIR}/apache/hive/2"

    download_jar "$url" "$dest_dir" "$artifact" "Apache Hive 2"
    download_hadoop_dependencies "$HADOOP_2_VERSION" "$dest_dir" "$MAVEN_CENTRAL" "Apache Hadoop for Apache Hive 2"
    download_log4j_dependencies "$dest_dir" "Apache Hive 2"
}

download_apache_hive_3() {
    local group_path="org/apache/hive/hive-jdbc"
    local artifact="hive-jdbc-${APACHE_3_VERSION}-standalone.jar"
    local url="${MAVEN_CENTRAL}/${group_path}/${APACHE_3_VERSION}/${artifact}"
    local dest_dir="${DRIVER_BASE_DIR}/apache/hive/3"

    download_jar "$url" "$dest_dir" "$artifact" "Apache Hive 3.1.3"
    download_hadoop_dependencies "$HADOOP_3_VERSION" "$dest_dir" "$MAVEN_CENTRAL" "Apache Hadoop for Apache Hive 3"
    download_log4j_dependencies "$dest_dir" "Apache Hive 3"
}

download_apache_hive_4() {
    local group_path="org/apache/hive/hive-jdbc"
    local artifact="hive-jdbc-${APACHE_4_VERSION}-standalone.jar"
    local url="${MAVEN_CENTRAL}/${group_path}/${APACHE_4_VERSION}/${artifact}"
    local dest_dir="${DRIVER_BASE_DIR}/apache/hive/4"

    download_jar "$url" "$dest_dir" "$artifact" "Apache Hive 4.0.1"
    download_hadoop_dependencies "$HADOOP_4_VERSION" "$dest_dir" "$MAVEN_CENTRAL" "Apache Hadoop for Apache Hive 4"
    download_log4j_dependencies "$dest_dir" "Apache Hive 4"
}

download_apache_hive_4_1() {
    local group_path="org/apache/hive/hive-jdbc"
    local artifact="hive-jdbc-${APACHE_4_1_VERSION}-standalone.jar"
    local url="${MAVEN_CENTRAL}/${group_path}/${APACHE_4_1_VERSION}/${artifact}"
    local dest_dir="${DRIVER_BASE_DIR}/apache/hive/4_1"

    download_jar "$url" "$dest_dir" "$artifact" "Apache Hive 4.1.0"
    download_hadoop_dependencies "$HADOOP_4_1_VERSION" "$dest_dir" "$MAVEN_CENTRAL" "Apache Hadoop for Hive 4.1"
    download_log4j_dependencies "$dest_dir" "Apache Hive 4.1"
}

download_cdh5() {
    local group_path="org/apache/hive/hive-jdbc"
    local artifact="hive-jdbc-${CDH5_HIVE_VERSION}-standalone.jar"
    local url="${CLOUDERA_REPO}/${group_path}/${CDH5_HIVE_VERSION}/${artifact}"
    local dest_dir="${DRIVER_BASE_DIR}/cdh/5"

    download_jar "$url" "$dest_dir" "$artifact" "Cloudera CDH 5 (Hive ${CDH5_HIVE_VERSION})"
    download_hadoop_dependencies "$CDH5_HADOOP_VERSION" "$dest_dir" "$CLOUDERA_REPO" "Hadoop for CDH 5"
}

download_cdh6() {
    local group_path="org/apache/hive/hive-jdbc"
    local artifact="hive-jdbc-${CDH6_HIVE_VERSION}-standalone.jar"
    local url="${CLOUDERA_REPO}/${group_path}/${CDH6_HIVE_VERSION}/${artifact}"
    local dest_dir="${DRIVER_BASE_DIR}/cdh/6"

    download_jar "$url" "$dest_dir" "$artifact" "Cloudera CDH 6 (Hive ${CDH6_HIVE_VERSION})"
    download_hadoop_dependencies "$CDH6_HADOOP_VERSION" "$dest_dir" "$CLOUDERA_REPO" "Hadoop for CDH 6"
}

download_hdp2() {
    local group_path="org/apache/hive/hive-jdbc"
    local artifact="hive-jdbc-${HDP2_VERSION}-standalone.jar"
    local url="${CLOUDERA_REPO}/${group_path}/${HDP2_VERSION}/${artifact}"
    local dest_dir="${DRIVER_BASE_DIR}/hdp/2"

    download_jar "$url" "$dest_dir" "$artifact" "Hortonworks HDP 2 (Hive ${HDP2_VERSION})"
}

download_cdp_7_1_9() {
    local group_path="org/apache/hive/hive-jdbc"
    local artifact="hive-jdbc-${CDP_7_1_9_VERSION}-standalone.jar"
    local url="${CLOUDERA_REPO}/${group_path}/${CDP_7_1_9_VERSION}/${artifact}"
    local dest_dir="${DRIVER_BASE_DIR}/cdp/7_1_9"

    download_jar "$url" "$dest_dir" "$artifact" "Cloudera CDP 7.1.9 (Hive ${CDP_7_1_9_VERSION})"
    download_hadoop_dependencies "$CDP_7_1_9_HADOOP" "$dest_dir" "$CLOUDERA_REPO" "Hadoop for CDP 7.1.9"
    download_log4j_dependencies "$dest_dir" "CDP 7.1.9"
}

download_cdp_7_3_1() {
    local group_path="org/apache/hive/hive-jdbc"
    local artifact="hive-jdbc-${CDP_7_3_1_VERSION}-standalone.jar"
    local url="${CLOUDERA_REPO}/${group_path}/${CDP_7_3_1_VERSION}/${artifact}"
    local dest_dir="${DRIVER_BASE_DIR}/cdp/7_3_1"

    download_jar "$url" "$dest_dir" "$artifact" "Cloudera CDP 7.3.1 (Hive ${CDP_7_3_1_VERSION})"
    download_hadoop_dependencies "$CDP_7_3_1_HADOOP" "$dest_dir" "$CLOUDERA_REPO" "Hadoop for CDP 7.3.1"
    download_log4j_dependencies "$dest_dir" "CDP 7.3.1"
}

################################################################################
# Main Logic
################################################################################

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--dir)
            DRIVER_BASE_DIR="$2"
            shift 2
            ;;
        -a|--all)
            DOWNLOAD_ALL=true
            shift
            ;;
        -s|--select)
            SELECTED_TYPES="$2"
            DOWNLOAD_ALL=false
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Display configuration
print_info "========================================="
print_info "HMS-Mirror Hive JDBC Driver Downloader"
print_info "========================================="
print_info "Installation directory: $DRIVER_BASE_DIR"
echo

# Check for required tools
if ! command -v curl &> /dev/null; then
    print_error "curl is required but not installed. Please install curl and try again."
    exit 1
fi

# Determine which drivers to download
FAILED_DOWNLOADS=()

if [ "$DOWNLOAD_ALL" = true ]; then
    print_info "Downloading all Hive JDBC drivers..."
    echo
#    download_apache_hive_1 || FAILED_DOWNLOADS+=("apache1")
#    echo
    download_apache_hive_2 || FAILED_DOWNLOADS+=("apache2")
    echo
    download_apache_hive_3 || FAILED_DOWNLOADS+=("apache3")
    echo
    download_apache_hive_4 || FAILED_DOWNLOADS+=("apache4")
    echo
    download_apache_hive_4_1 || FAILED_DOWNLOADS+=("apache4_1")
    echo
    download_cdh5 || FAILED_DOWNLOADS+=("cdh5")
    echo
    download_cdh6 || FAILED_DOWNLOADS+=("cdh6")
    echo
    download_hdp2 || FAILED_DOWNLOADS+=("hdp2")
    echo
    download_cdp_7_1_9 || FAILED_DOWNLOADS+=("cdp7_1_9")
    echo
    download_cdp_7_3_1 || FAILED_DOWNLOADS+=("cdp7_3_1")
else
    IFS=',' read -ra TYPES <<< "$SELECTED_TYPES"
    for type in "${TYPES[@]}"; do
        case $type in
#            apache1)
#                download_apache_hive_1 || FAILED_DOWNLOADS+=("apache1")
#                ;;
            apache2)
                download_apache_hive_2 || FAILED_DOWNLOADS+=("apache2")
                ;;
            apache3)
                download_apache_hive_3 || FAILED_DOWNLOADS+=("apache3")
                ;;
            apache4)
                download_apache_hive_4 || FAILED_DOWNLOADS+=("apache4")
                ;;
            apache4_1)
                download_apache_hive_4_1 || FAILED_DOWNLOADS+=("apache4_1")
                ;;
            cdh5)
                download_cdh5 || FAILED_DOWNLOADS+=("cdh5")
                ;;
            cdh6)
                download_cdh6 || FAILED_DOWNLOADS+=("cdh6")
                ;;
            hdp2)
                download_hdp2 || FAILED_DOWNLOADS+=("hdp2")
                ;;
            cdp7_1_9)
                download_cdp_7_1_9 || FAILED_DOWNLOADS+=("cdp7_1_9")
                ;;
            cdp7_3_1)
                download_cdp_7_3_1 || FAILED_DOWNLOADS+=("cdp7_3_1")
                ;;
            all)
                DOWNLOAD_ALL=true
#                download_apache_hive_1 || FAILED_DOWNLOADS+=("apache1")
                download_apache_hive_2 || FAILED_DOWNLOADS+=("apache2")
                download_apache_hive_3 || FAILED_DOWNLOADS+=("apache3")
                download_apache_hive_4 || FAILED_DOWNLOADS+=("apache4")
                download_apache_hive_4_1 || FAILED_DOWNLOADS+=("apache4_1")
                download_cdh5 || FAILED_DOWNLOADS+=("cdh5")
                download_cdh6 || FAILED_DOWNLOADS+=("cdh6")
                download_hdp2 || FAILED_DOWNLOADS+=("hdp2")
                download_cdp_7_1_9 || FAILED_DOWNLOADS+=("cdp7_1_9")
                download_cdp_7_3_1 || FAILED_DOWNLOADS+=("cdp7_3_1")
                ;;
            *)
                print_warning "Unknown driver type: $type (skipping)"
                ;;
        esac
        echo
    done
fi

# Summary
echo
print_info "========================================="
print_info "Download Summary"
print_info "========================================="

if [ ${#FAILED_DOWNLOADS[@]} -eq 0 ]; then
    print_success "All drivers downloaded successfully!"
    print_info "Drivers installed to: $DRIVER_BASE_DIR"
    echo
    print_info "You can now configure HMS-Mirror to use these drivers by setting:"
    print_info "  hms-mirror.drivers.base-dir=${DRIVER_BASE_DIR}"
    exit 0
else
    print_warning "Some downloads failed:"
    for failed in "${FAILED_DOWNLOADS[@]}"; do
        print_error "  - $failed"
    done
    echo
    print_info "Successfully downloaded drivers are available in: $DRIVER_BASE_DIR"
    print_warning "You may need to manually download the failed drivers or check repository URLs."
    exit 1
fi
