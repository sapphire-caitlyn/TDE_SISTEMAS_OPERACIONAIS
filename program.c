#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

long NUMBER_PROCESSORS;
const char* FILE_NAME = "devices.test.csv";

#define MAX_LINE_SIZE 1024
#define MAX_RECORDS 1000

//Last line = 4_199_708
// Estrutura para armazenar uma linha de dados do CSV
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
    usleep(5000);
  }
  fclose(file);

  return lstData;
}

int main() {  
  NUMBER_PROCESSORS = sysconf(_SC_NPROCESSORS_ONLN);

  int rows;
  IoTData* lstData = FileToData(FILE_NAME, &rows);

  int* lstDeviceCounts;
  int maxDeviceId;
  // Free the allocated memory
  free(lstData);
  
  return 0;
}