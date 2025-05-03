#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <float.h>
#include <time.h>

long NUMBER_PROCESSORS;
char* FILE_NAME = "devices.final.csv";
char* OUTPUT_FILE_NAME = "output.devices.csv";

#define MAX_LINE_SIZE 1024
#define MAX_RECORDS 1000

//Last line = 4_199_708
// Estrutura para armazenar uma linha de dados do CSV
#pragma region ReadCSV
typedef struct {
  int id;
  int idDevice;
  int contagem;
  char data[27];
  float temperatura;
  float umidade;
  float luminosidade;
  float ruido;
  float eco2;
  float etvoc;
  float latitude;
  float longitude;
  int day;
  int month;  
  int year;   
} IoTData;

// Function to trim whitespace from a string
void Trim(char *str) {
  if (str == NULL) return;
  
  char *start = str;
  char *end = str + strlen(str) - 1;
  
  while (*start && (*start == ' ' || *start == '\t')) start++;
  while (end > start && (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r')) *end-- = '\0';
  
  if (start != str) {
      memmove(str, start, strlen(start) + 1);
  }
}

// Function to parse date string and extract year, month, and day
void ParseDate(const char *dateStr, int *year, int *month, int *day) {
  // Initialize with invalid values
  *year = 0;
  *month = 0;
  *day = 0;
  
  // Check if date string is valid
  if (dateStr == NULL || strlen(dateStr) < 10) {
      return;  // Not enough characters for YYYY-MM-DD
  }
  
  // Extract year (first 4 characters)
  char yearStr[5];
  strncpy(yearStr, dateStr, 4);
  yearStr[4] = '\0';
  *year = atoi(yearStr);
  
  // Check if the format is correct (hyphen at position 4)
  if (dateStr[4] != '-') {
      *year = 0;
      return;
  }
  
  // Extract month (characters 5-6)
  char monthStr[3];
  strncpy(monthStr, dateStr + 5, 2);
  monthStr[2] = '\0';
  *month = atoi(monthStr);
  
  // Check if the format is correct (hyphen at position 7)
  if (dateStr[7] != '-') {
      *year = 0;
      *month = 0;
      return;
  }
  
  // Extract day (characters 8-9)
  char dayStr[3];
  strncpy(dayStr, dateStr + 8, 2);
  dayStr[2] = '\0';
  *day = atoi(dayStr);
}


int ExtractDeviceID(const char *device) {
  if (device == NULL || strlen(device) == 0) {
      return -1;  // Invalid device name
  }
  
  // Find the last occurrence of '-'
  const char *hyphen = strrchr(device, '-');
  if (hyphen == NULL) {
      return -1;  // No hyphen found
  }
  
  // Move past the hyphen
  hyphen++;
  
  // Check if there's anything after the hyphen
  if (*hyphen == '\0') {
      return -1;  // Nothing after hyphen
  }
  
  // Convert the substring to an integer
  int device_id = atoi(hyphen);
  
  return device_id;
}

// Function to parse a CSV line into an IoTData struct using strpbrk
void ParseCSVLine(char *line, IoTData *data) {
  char *current = line;
  char *next;
  char field[256];
  int fieldIndex = 0;

  // Initialize the struct with default values
  memset(data, 0, sizeof(IoTData));

  // Process each field
  while (current != NULL && fieldIndex < 12) {
    // Find the next delimiter
    next = strpbrk(current, "|");

    if (next != NULL) {
      // Extract the field value
      size_t length = next - current;
      if (length > 0 && length < sizeof(field)) {
        strncpy(field, current, length);
        field[length] = '\0';
      } else {
        // Empty field
        field[0] = '\0';
      }

      // Move to the next field
      current = next + 1;
    } else {
      // Last field
      if (strlen(current) > 0 && strlen(current) < sizeof(field)) {
        strcpy(field, current);
        Trim(field);
      } else {
        field[0] = '\0';
      }
      current = NULL;
    }

    Trim(field);

    // Process the field based on its index
    switch (fieldIndex) {
    case 0: // id
      if (strlen(field) > 0) {
        data->id = atoi(field);
      }
      break;
    case 1: // device (extract ID but don't store the name)
      if (strlen(field) > 0) {
        // Extract device ID from the device name
        data->idDevice = ExtractDeviceID(field);
      }
      break;
    case 2: // contagem
      if (strlen(field) > 0) {
        data->contagem = atoi(field);
      }
      break;
    case 3: // data
      if (strlen(field) > 0) {
        strncpy(data->data, field, sizeof(data->data) - 1);
        data->data[sizeof(data->data) - 1] = '\0';
        
        // Parse the date string to extract year, month, and day
        ParseDate(field, &data->year, &data->month, &data->day);
      }
      break;
    case 4: // temperatura
      if (strlen(field) > 0) {
        data->temperatura = atof(field);
      }
      break;
    case 5: // umidade
      if (strlen(field) > 0) {
        data->umidade = atof(field);
      }
      break;
    case 6: // luminosidade
      if (strlen(field) > 0) {
        data->luminosidade = atof(field);
      }
      break;
    case 7: // ruido
      if (strlen(field) > 0) {
        data->ruido = atof(field);
      }
      break;
    case 8: // eco2
      if (strlen(field) > 0) {
        data->eco2 = atof(field);
      }
      break;
    case 9: // etvoc
      if (strlen(field) > 0) {
        data->etvoc = atof(field);
      }
      break;
    case 10: // latitude
      if (strlen(field) > 0) {
        data->latitude = atof(field);
      }
      break;
    case 11: // longitude
      if (strlen(field) > 0) {
        data->longitude = atof(field);
      }
      break;
    }

    fieldIndex++;
  }
}

int CountLines(FILE *file) {
  int lines = 1;
  char ch;
  while (!feof(file)) {
    ch = fgetc(file);
    if (ch == '\n') {
      lines++;
    }
  }
  rewind(file); // Reset file pointer to the beginning
  return lines;
}

void ShowProgressBar(int percent) {
    int barWidth = 50; // Adjust the width of the progress bar
    int pos = (percent * barWidth) / 100;
  
    printf("\r[");
    for (int i = 0; i < barWidth; ++i) {
      if (i < pos)
          printf("#");
      else if (i == pos)
          printf("@");
      else
          printf(" ");
    }
    printf("] %d%%", percent);
    fflush(stdout); // Force output flush
  }

IoTData* FileToData(char* fileName, int* rowsPtr) {
  printf("[FileToData] [1 out of 3] Started program, opening file...\n");
  // Open the file
  FILE *file;
  file = fopen(FILE_NAME , "r");

  int rows = CountLines(file) - 1; // Exclude header line
  printf("[FileToData] [2 out of 3] Number of records: %d\n", rows);
  // Allocate memory for the array of IoTData structs
  IoTData* lstData = (IoTData*)malloc(rows * sizeof(IoTData));
  char line[255];
  
  // Discard the header line
  fgets(line, sizeof(line), file);

  printf("[FileToData] [3 out of 3] Parsing CSV file...\n");
  for(int i = 0; i < rows; i++) {
    fgets(line, sizeof(line), file);
    if (strlen(line) <= 1) {
      continue;
    }
    ParseCSVLine(line, &lstData[i]);
    // Show progress bar
    int percent = (i + 1) * 100 / rows;
    ShowProgressBar(percent);
  }
  fclose(file);

  *rowsPtr = rows;
  return lstData;
}
#pragma endregion ReadCSV

#pragma region SortIntoMatrix
// Structure to hold thread arguments
typedef struct {
  IoTData* lstData;
  IoTData*** result_matrix;
  int* counts;
  int start_idx;
  int end_idx;
  int max_device_id;
  pthread_mutex_t* lstLock;
} ThreadArgs;

// Function to find the maximum device ID in the array
int find_max_device_id(IoTData* array, int size) {
  int max_id = 0;
  for (int i = 0; i < size; i++) {
    if (array[i].idDevice > max_id) {
      max_id = array[i].idDevice;
    }
  }
  return max_id;
}

// Thread function to sort a portion of the array
void* SortData(void* arg) {
  ThreadArgs* args = (ThreadArgs*)arg;
  
  // Process assigned portion of the array
  for (int i = args->start_idx; i < args->end_idx; i++) {
    int device_id = args->lstData[i].idDevice;
    
    // Skip invalid device IDs
    if (device_id < 0 || device_id > args->max_device_id) {
      printf("Warning: Invalid device ID %d found. Skipping.\n", device_id);
      continue;
    }
    
    // Lock the mutex for this device ID to avoid race conditions
    pthread_mutex_lock(&args->lstLock[device_id]);
    
    // Allocate or reallocate memory for this device's array
    int current_count = args->counts[device_id];
    args->result_matrix[device_id] = realloc(args->result_matrix[device_id], 
                                            (current_count + 1) * sizeof(IoTData*));
    
    // Allocate memory for the new IoTData entry
    args->result_matrix[device_id][current_count] = malloc(sizeof(IoTData));
    
    // Copy the data
    memcpy(args->result_matrix[device_id][current_count], &args->lstData[i], sizeof(IoTData));
    
    // Increment the count
    args->counts[device_id]++;
    
    // Unlock the mutex
    pthread_mutex_unlock(&args->lstLock[device_id]);
  }
  
  return NULL;
}

IoTData*** Sorting(IoTData* lstData, int rows, int* max_device_idPtr, int** countsPtr) {
  // Start sorting the data into a matrix
  printf("[Sorting] [1 out of 3] Sorting data into matrix...\n");
  int max_device_id = find_max_device_id(lstData, rows);
  // Allocate the result matrix and counts array
  IoTData*** result_matrix = malloc((max_device_id + 1) * sizeof(IoTData**));
  int* counts = calloc(max_device_id + 1, sizeof(int));

  if (!result_matrix || !counts) {
    printf("Memory allocation failed for result matrix or counts.\n");
    free(lstData);
    return NULL;
  }

  // Initialize the result matrix
  for (int i = 0; i <= max_device_id; i++) {
    result_matrix[i] = NULL; // Will be allocated as needed
  }

  // Create mutexes for synchronization
  pthread_mutex_t* lstLock = malloc((max_device_id + 1) * sizeof(pthread_mutex_t));
  if (!lstLock) {
    printf("Memory allocation failed for lstLock.\n");
    free(lstData);
    free(result_matrix);
    free(counts);
    return NULL;
  }

  for (int i = 0; i <= max_device_id; i++) {
    pthread_mutex_init(&lstLock[i], NULL);
  }

  // Create threads
  pthread_t* threads      = malloc(NUMBER_PROCESSORS * sizeof(pthread_t));
  ThreadArgs* thread_args = malloc(NUMBER_PROCESSORS * sizeof(ThreadArgs));

  if (!threads || !thread_args) {
    printf("Memory allocation failed for threads or thread arguments.\n");
    free(lstData);
    free(result_matrix);
    free(counts);
    free(lstLock);
    return NULL;
  }

  // Calculate workload per thread
  int items_per_thread = rows / NUMBER_PROCESSORS;
  int remainder        = rows % NUMBER_PROCESSORS;

  printf("[Sorting] [2 out of 3] Creating %ld threads to process %d items...\n", NUMBER_PROCESSORS, rows);

  int start_idx = 0;
  for (int i = 0; i < NUMBER_PROCESSORS; i++) {
    thread_args[i].lstData       = lstData;
    thread_args[i].result_matrix = result_matrix;
    thread_args[i].counts        = counts;
    thread_args[i].start_idx     = start_idx;
    thread_args[i].max_device_id = max_device_id;
    thread_args[i].lstLock       = lstLock;
    
    // Distribute the remainder among the first 'remainder' threads
    int items_for_this_thread = items_per_thread + (i < remainder ? 1 : 0);
    thread_args[i].end_idx = start_idx + items_for_this_thread;
    
    // Create the thread
    if (pthread_create(&threads[i], NULL, SortData, &thread_args[i]) != 0) {
      printf("Failed to create thread %d\n", i);
      // Clean up and exit
      for (int j = 0; j < i; j++) {
        pthread_cancel(threads[j]);
        pthread_join(threads[j], NULL);
      }
      free(lstData);
      free(result_matrix);
      free(counts);
      free(lstLock);
      free(threads);
      free(thread_args);
      return NULL;
    }
    
    start_idx += items_for_this_thread;
  }

  // Wait for all threads to complete
  for (int i = 0; i < NUMBER_PROCESSORS; i++) {
    pthread_join(threads[i], NULL);
  }

  printf("[Sorting] [3 out of 3] All threads completed. Printing results...\n");

  // // Print the results
  // for (int i = 0; i <= max_device_id; i++) {
  //   printf("Device ID %d has %d entries\n", i, counts[i]);
  //   for (int j = 0; j < counts[i]; j++) {
  //     printf("Entry %d: ID=%d, DeviceID=%d, Date=%s, Temp=%.2f, Humidity=%.2f\n",
  //            j, result_matrix[i][j]->id, result_matrix[i][j]->idDevice,
  //            result_matrix[i][j]->data, result_matrix[i][j]->temperatura,
  //            result_matrix[i][j]->umidade);
  //   }
  // }

  // Clean up
  printf("[ Cleaning up resources... ]\n");
  
  // Destroy mutexes
  for (int i = 0; i <= max_device_id; i++) {
    pthread_mutex_destroy(&lstLock[i]);
  }
  // free(lstLock);
  // free(threads);
  // free(thread_args);

  *max_device_idPtr = max_device_id;
  *countsPtr        = counts;
  return result_matrix;
}

#pragma endregion SortIntoMatrix

#pragma region Results
typedef struct {
  int idDevice;
  int ano;
  int mes;
  int sensor; // 1-temperatura, 2-umidade, 3-luminosidade, 4-ruido, 5-eco2, 6-etvoc
  float max;
  float avg;
  float min;
} StatResult;

// Global variables
IoTData*** result_matrix;
int num_devices;
int* device_data_counts;
pthread_mutex_t result_mutex;

// Array to store results
StatResult* results;
int result_count = 0;
int max_results = 0;

// Structure to represent a month-year combination
typedef struct {
  int month;
  int year;
} MonthYear;

// Array to store unique month-year combinations
MonthYear* month_years;
int month_year_count = 0;

// Thread argument structure
typedef struct {
  int start_idx;
  int end_idx;
} StatThreadArgs;

// Function to get sensor value based on sensor type
float get_sensor_value(IoTData* data, int sensor_type) {
  switch (sensor_type) {
    case 1: return data->temperatura;
    case 2: return data->umidade;
    case 3: return data->luminosidade;
    case 4: return data->ruido;
    case 5: return data->eco2;
    case 6: return data->etvoc;
    default: return 0.0f;
  }
}

// Function to add a result to the results array
void add_result(StatResult result) {
  pthread_mutex_lock(&result_mutex);
  
  if (result_count >= max_results) {
    max_results = max_results == 0 ? 100 : max_results * 2;
    results = realloc(results, max_results * sizeof(StatResult));
    if (!results) {
      fprintf(stderr, "Memory allocation failed\n");
      exit(1);
    }
  }
  
  results[result_count++] = result;
  
  pthread_mutex_unlock(&result_mutex);
}

// Compare function for qsort to sort MonthYear structs
int compare_month_year(const void* a, const void* b) {
  const MonthYear* my_a = (const MonthYear*)a;
  const MonthYear* my_b = (const MonthYear*)b;
  
  if (my_a->year != my_b->year) {
    return my_a->year - my_b->year;
  }
  return my_a->month - my_b->month;
}

// Function to find all unique month-year combinations in the data
void find_unique_month_years() {
  // Allocate initial array
  int max_month_years = 100;
  month_years = malloc(max_month_years * sizeof(MonthYear));
  if (!month_years) {
    fprintf(stderr, "Memory allocation failed\n");
    exit(1);
  }
  
  // Temporary array to track which month-years we've already seen
  int max_year = 0, min_year = 9999;
  
  // Find min and max years to determine array size
  for (int i = 0; i < num_devices; i++) {
    for (int j = 0; j < device_data_counts[i]; j++) {
      IoTData* data = result_matrix[i][j];
      if (data->year > max_year) max_year = data->year;
      if (data->year < min_year) min_year = data->year;
    }
  }
  
  // Create a bitmap to track which month-years we've seen
  // Each year has 12 months, so we need (max_year - min_year + 1) * 12 bits
  int bitmap_size = (max_year - min_year + 1) * 12;
  char* bitmap = calloc((bitmap_size + 7) / 8, 1); // +7 to round up to nearest byte
  
  // Collect unique month-years
  for (int i = 0; i < num_devices; i++) {
    for (int j = 0; j < device_data_counts[i]; j++) {
      IoTData* data = result_matrix[i][j];
      
      // Calculate bit position
      int bit_pos = ((data->year - min_year) * 12 + (data->month - 1));
      int byte_pos = bit_pos / 8;
      int bit_offset = bit_pos % 8;
      
      // Check if we've seen this month-year before
      if (!(bitmap[byte_pos] & (1 << bit_offset))) {
        // Mark as seen
        bitmap[byte_pos] |= (1 << bit_offset);
        
        // Add to array
        if (month_year_count >= max_month_years) {
          max_month_years *= 2;
          month_years = realloc(month_years, max_month_years * sizeof(MonthYear));
          if (!month_years) {
            fprintf(stderr, "Memory allocation failed\n");
            exit(1);
          }
        }
        
        month_years[month_year_count].month = data->month;
        month_years[month_year_count].year = data->year;
        month_year_count++;
      }
    }
  }
  
  // Sort month-years chronologically
  qsort(month_years, month_year_count, sizeof(MonthYear), compare_month_year);
  
  free(bitmap);
}

// Worker thread function
void* process_month_year_range(void* arg) {
  StatThreadArgs* args = (StatThreadArgs*)arg;
  int start_idx = args->start_idx;
  int end_idx = args->end_idx;
  
  // Process each month-year in the assigned range
  for (int my_idx = start_idx; my_idx <= end_idx; my_idx++) {
    int month = month_years[my_idx].month;
    int year = month_years[my_idx].year;
    
    // Process each device separately
    for (int dev_idx = 1; dev_idx < num_devices; dev_idx++) {
      IoTData** device_data = result_matrix[dev_idx];
      int device_id = device_data[0]->idDevice; // All entries for this device have the same ID
      
      // Process each sensor type (1-6)
      for (int sensor_type = 1; sensor_type <= 6; sensor_type++) {
        float sum = 0.0f;
        float max_val = -FLT_MAX;
        float min_val = FLT_MAX;
        int count = 0;
        
        // Process all data points for this device, month, year, and sensor type
        for (int i = 0; i < device_data_counts[dev_idx]; i++) {
          IoTData* data = device_data[i];
          
          // Check if this data point matches the current month and year
          if (data->month == month && data->year == year) {
            float value = get_sensor_value(data, sensor_type);
            
            // Update statistics
            sum += value;
            if (value > max_val) max_val = value;
            if (value < min_val) min_val = value;
            count++;
          }
        }
        
        // If we found data points for this device/month/year/sensor, add a result
        if (count > 0) {
          StatResult result;
          result.idDevice = device_id;
          result.ano = year;
          result.mes = month;
          result.sensor = sensor_type;
          result.max = max_val;
          result.min = min_val;
          result.avg = sum / count;
          
          add_result(result);
        }
      }
    }
  }
  
  free(args);
  return NULL;
}

// Main function to process all data
void process_iot_data(int num_threads) {
  // Initialize mutex
  pthread_mutex_init(&result_mutex, NULL);
  
  // Allocate results array
  max_results = 100;
  results = malloc(max_results * sizeof(StatResult));
  if (!results) {
    fprintf(stderr, "Memory allocation failed\n");
    exit(1);
  }
  
  // Find all unique month-year combinations
  find_unique_month_years();
  
  // Create threads
  pthread_t* threads = malloc(num_threads * sizeof(pthread_t));
  if (!threads) {
    fprintf(stderr, "Memory allocation failed\n");
    exit(1);
  }
  
  // Distribute work among threads by month-year ranges
  int items_per_thread = (month_year_count + num_threads - 1) / num_threads;
  
  for (int t = 0; t < num_threads; t++) {
    StatThreadArgs* args = malloc(sizeof(StatThreadArgs));
    if (!args) {
      fprintf(stderr, "Memory allocation failed\n");
      exit(1);
    }
    
    args->start_idx = t * items_per_thread;
    args->end_idx = (t + 1) * items_per_thread - 1;
    
    // Make sure we don't go past the end of the array
    if (args->end_idx >= month_year_count) {
      args->end_idx = month_year_count - 1;
    }
    
    // If this thread has no work, don't create it
    if (args->start_idx > args->end_idx) {
      free(args);
      continue;
    }
    
    pthread_create(&threads[t], NULL, process_month_year_range, args);
  }
  
  // Wait for all threads to complete
  for (int t = 0; t < num_threads; t++) {
    // Only join threads that were actually created
    if (t * items_per_thread < month_year_count) {
      pthread_join(threads[t], NULL);
    }
  }
  
  // Clean up
  free(threads);
  free(month_years);
  pthread_mutex_destroy(&result_mutex);
}

// Function to print results
void print_results() {
  FILE* output_file = fopen(OUTPUT_FILE_NAME, "w");
  fprintf(output_file, "idDevice;ano_mes;sensor;valor_max;valor_medio;valor_min\n");

  printf("Total results: %d\n", result_count);
  printf("Device\tYear\tMonth\tSensor\tMin\tAvg\tMax\n");
  
  for (int i = 0; i < result_count; i++) {
    StatResult* r = &results[i];
    const char* sensor_name;
    
    switch (r->sensor) {
      case 1: sensor_name = "temperatura"; break;
      case 2: sensor_name = "umidade"; break;
      case 3: sensor_name = "luminosidade"; break;
      case 4: sensor_name = "ruido"; break;
      case 5: sensor_name = "eco2"; break;
      case 6: sensor_name = "etvoc"; break;
      default: sensor_name = "unknown"; break;
    }
    
    fprintf(output_file, "%d;%d/%d;%s;%.2f;%.2f;%.2f\n", 
           r->idDevice, r->ano, r->mes, sensor_name, r->max, r->avg, r->min);
    printf("%d\t%d\t%d\t%s\t%.2f\t%.2f\t%.2f\n", 
           r->idDevice, r->ano, r->mes, sensor_name, r->min, r->avg, r->max);
  }
}

#pragma endregion Results

int main() {  
  NUMBER_PROCESSORS = sysconf(_SC_NPROCESSORS_ONLN);

  clock_t begin = clock();  
  // Read the CSV file and parse it into an array of IoTData structs
  int rows;
  IoTData* lstData = FileToData(FILE_NAME, &rows);
  clock_t end = clock();
  printf("\n[Main] Time spent: %f seconds\n", (double)(end - begin) / CLOCKS_PER_SEC);

  int max_device_id; 
  int* counts; // Count the data for each device
  result_matrix = Sorting(lstData, rows, &max_device_id, &counts);
  num_devices = max_device_id + 1;
  device_data_counts = counts;

  // Results
  process_iot_data(NUMBER_PROCESSORS);

  // Print results
  print_results();


  free(lstData);
  for (int i = 0; i <= max_device_id; i++) {
    for (int j = 0; j < counts[i]; j++) {
      free(result_matrix[i][j]);
    }
    free(result_matrix[i]);
  }
  free(result_matrix);
  free(counts);
  
  return 0;
}