// Copyright 2025 kg.sai. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cycau.dragonfly5.rdbclient;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HaveError extends Exception {
    @JsonProperty("errcode")
    private String errCode;
    @JsonProperty("message")
    private String message;

    public HaveError() { super(); }

    public HaveError(String errCode, String message) {
        super(message);
        this.errCode = errCode;
        this.message = message;
    }

    public String getErrCode() { return errCode; }
    public void setErrCode(String errCode) { this.errCode = errCode; }
    @Override
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    @Override
    public String toString() {
        return "HaveError{errCode='" + errCode + "', message='" + message + "'}";
    }
}
